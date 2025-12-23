# 파일: train_fault_model.py
import os
import argparse
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
import shutil, json

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score

import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

# ----------------- 설정 -----------------

PG_URL = os.getenv(
    "PG_URL",
    "postgresql+psycopg2://phm_user:phm-password@localhost:5432/phm",
)
engine = create_engine(PG_URL)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_EXPERIMENT_NAME = "phm_vibration_fault_diagnosis"


# ----------------- 세션 상태 업데이트 -----------------


def mark_sessions_trained(session_ids: list[int]):
    """
    이번 학습에 사용된 session_id 목록에 대해 train_done_at 을 기록.
    (추후 재학습 여부 판단용)
    """
    if not session_ids:
        return

    sql = text(
        """
        UPDATE session
        SET train_done_at = NOW()
        WHERE id = ANY(:ids);
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"ids": session_ids})


# ----------------- 데이터 로드 -----------------


def load_feature_df() -> pd.DataFrame:
    """
    mart.vibration_frame_features 에서 결함 진단용 학습 데이터 로드.

    - task_type 이 fault_diag / fault_diagnosis 인 것만 사용
    - axis 는 x, y, z 모두 사용해서 프레임 단위로 병합 (3축 15차원)
    - target = label_type (normal / fault_A / fault_B / ...)
    """
    base_sql = """
        SELECT
            session_id,
            device_id,
            frame_seq,
            axis,
            rms,
            peak,
            mean_abs,
            std,
            crest_factor,
            task_type,
            label_type,
            data_split
        FROM mart.vibration_frame_features
        WHERE axis IN ('x', 'y', 'z')
          AND label_type IS NOT NULL
          AND task_type IN ('fault_diag', 'fault_diagnosis')
    """

    query = text(base_sql)

    with engine.connect() as conn:
        df_long = pd.read_sql_query(query, conn)

    if df_long.empty:
        return df_long

    # ---------- 메타 정보 (세션/프레임 단위 고정 값) ----------
    meta_cols = [
        "session_id",
        "device_id",
        "frame_seq",
        "task_type",
        "label_type",
        "data_split",
    ]

    meta_df = (
        df_long[meta_cols]
        .drop_duplicates(subset=["session_id", "device_id", "frame_seq"])
        .reset_index(drop=True)
    )

    # ---------- feature 를 x,y,z 축으로 피벗 (long -> wide) ----------
    feature_cols = ["rms", "peak", "mean_abs", "std", "crest_factor"]

    feat_pivot = df_long.pivot_table(
        index=["session_id", "device_id", "frame_seq"],
        columns="axis",
        values=feature_cols,
        aggfunc="first",
    )

    # ('rms', 'x') -> 'rms_x' 식으로 평탄화
    feat_pivot.columns = [
        f"{feat}_{ax}" for (feat, ax) in feat_pivot.columns.to_list()
    ]
    feat_pivot = feat_pivot.reset_index()

    # ---------- 메타 + feature merge ----------
    df_wide = pd.merge(
        meta_df,
        feat_pivot,
        on=["session_id", "device_id", "frame_seq"],
        how="inner",
    )

    # x, y, z 모두 있는 프레임만 사용 (NaN 포함 프레임 제거)
    full_feature_cols = [
        f"{feat}_{ax}"
        for feat in feature_cols
        for ax in ("x", "y", "z")
    ]
    df_wide = df_wide.dropna(subset=full_feature_cols)

    return df_wide


def prepare_dataset(df: pd.DataFrame):
    """
    DataFrame(wide) -> (X_train, X_val, X_test, y_train, y_val, y_test)

    - target: label_type (normal / fault_A / fault_B / ...)
    - feature: 3축 × 5개 = 15차원
    - data_split 컬럼이 'train'/'val'/'test' 로 채워져 있으면 그대로 사용
    - 아니면 train_test_split 으로 랜덤 분할 (stratify)
    """
    if df.empty:
        print("[TRAIN-FAULT] no data in mart.vibration_frame_features (after query), abort")
        return None

    df = df.copy()
    df["label_type"] = df["label_type"].astype(str)

    base_feats = ["rms", "peak", "mean_abs", "std", "crest_factor"]
    feature_cols = [
        f"{feat}_{ax}"
        for feat in base_feats
        for ax in ("x", "y", "z")
    ]

    missing_cols = [c for c in feature_cols if c not in df.columns]
    if missing_cols:
        print(f"[TRAIN-FAULT][ERROR] missing feature columns: {missing_cols}")
        return None

    X = df[feature_cols]
    y = df["label_type"]

    # 레이블 분포 로그
    print("[TRAIN-FAULT] label distribution (label_type):")
    print(y.value_counts())

    has_split_col = "data_split" in df.columns
    has_any_split_value = df["data_split"].notnull().any() if has_split_col else False

    if has_split_col and has_any_split_value:
        print("[TRAIN-FAULT] using data_split column for train/val/test split")

        train_mask = df["data_split"] == "train"
        val_mask = df["data_split"] == "val"
        test_mask = df["data_split"] == "test"

        if not train_mask.any():
            print(
                "[TRAIN-FAULT][WARN] no rows with data_split='train', "
                "fallback to random split"
            )
        else:
            X_train, y_train = X[train_mask], y[train_mask]
            X_val, y_val = (
                (X[val_mask], y[val_mask]) if val_mask.any() else (None, None)
            )
            X_test, y_test = (
                (X[test_mask], y[test_mask]) if test_mask.any() else (None, None)
            )

            if (
                X_val is None
                or len(X_val) == 0
                or X_test is None
                or len(X_test) == 0
            ):
                print(
                    "[TRAIN-FAULT][WARN] val/test empty or too small, "
                    "fallback to random split"
                )
            else:
                print(
                    f"[TRAIN-FAULT] split by data_split: "
                    f"train={len(X_train)}, val={len(X_val)}, test={len(X_test)}"
                )
                return X_train, X_val, X_test, y_train, y_val, y_test

    # fallback: 랜덤 분할
    print("[TRAIN-FAULT] random train/val/test split by sklearn")

    X_train, X_tmp, y_train, y_tmp = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y
    )
    X_val, X_test, y_val, y_test = train_test_split(
        X_tmp, y_tmp, test_size=0.5, random_state=42, stratify=y_tmp
    )

    print(
        f"[TRAIN-FAULT] random split: train={len(X_train)}, "
        f"val={len(X_val)}, test={len(X_test)}"
    )
    return X_train, X_val, X_test, y_train, y_val, y_test


# ----------------- 학습 로직 -----------------


def build_model(model_type: str) -> RandomForestClassifier:
    """
    model_type 에 따라 다른 모델 구성 가능하도록 확장 포인트.
    지금은 rf 하나만 지원.
    """
    model_type = model_type.lower()
    if model_type in ("rf", "random_forest", "randomforest"):
        return RandomForestClassifier(
            n_estimators=200,
            max_depth=None,
            n_jobs=-1,
            random_state=42,
        )
    else:
        raise ValueError(f"Unsupported model_type: {model_type}")


def train_and_log(model_type: str):
    """
    label_type 기반 결함 진단 모델 학습 & MLflow 로그
    (입력: 3축 15차원 feature)
    """
    df = load_feature_df()
    if df.empty:
        print(
            "[TRAIN-FAULT] no data for task_type in (fault_diag, fault_diagnosis), abort"
        )
        return

    print(f"[TRAIN-FAULT] loaded features df shape = {df.shape}")

    dataset = prepare_dataset(df)
    if dataset is None:
        return

    X_train, X_val, X_test, y_train, y_val, y_test = dataset

    # 이번 학습에 실제로 사용된 세션 id 목록
    used_session_ids = sorted(df["session_id"].unique().tolist())
    print(f"[TRAIN-FAULT] used sessions: {len(used_session_ids)} sessions")

    model = build_model(model_type)

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    run_name = (
        f"{model_type}_axis_xyz_fault_diag_"
        f"{pd.Timestamp.utcnow():%Y%m%d_%H%M%S}"
    )

    with mlflow.start_run(run_name=run_name):
        # -------- 파라미터 기록 --------
        mlflow.log_param("model_type", model_type)
        mlflow.log_param("n_estimators", getattr(model, "n_estimators", None))
        mlflow.log_param("max_depth", getattr(model, "max_depth", None))
        mlflow.log_param("features", "rms,peak,mean_abs,std,crest_factor (x,y,z 3축)")
        mlflow.log_param("axis", "xyz_combined")
        mlflow.log_param("label_target", "label_type")
        mlflow.log_param("num_sessions", len(used_session_ids))
        mlflow.log_param("task_type", "fault_diagnosis")

        # -------- 학습 --------
        model.fit(X_train, y_train)

        # -------- train / val / test accuracy --------
        y_train_pred = model.predict(X_train)
        train_acc = accuracy_score(y_train, y_train_pred)
        mlflow.log_metric("train_accuracy", float(train_acc))
        mlflow.log_metric("train_acc", float(train_acc))

        val_acc = None
        test_acc = None
        macro_f1 = None

        if X_val is not None and len(X_val) > 0:
            y_val_pred = model.predict(X_val)
            val_acc = accuracy_score(y_val, y_val_pred)
            mlflow.log_metric("val_accuracy", float(val_acc))
            mlflow.log_metric("val_acc", float(val_acc))
        else:
            print("[TRAIN-FAULT][WARN] no validation set, skip val metrics")

        if X_test is not None and len(X_test) > 0:
            y_test_pred = model.predict(X_test)
            test_acc = accuracy_score(y_test, y_test_pred)
            mlflow.log_metric("test_accuracy", float(test_acc))
            mlflow.log_metric("test_acc", float(test_acc))

            cls_report = classification_report(
                y_test, y_test_pred, output_dict=True
            )
            macro_f1 = cls_report["macro avg"]["f1-score"]
            mlflow.log_metric("macro_f1", float(macro_f1))
        else:
            print("[TRAIN-FAULT][WARN] no test set, skip test metrics & macro_f1")

        # -------- 모델 저장 --------
        signature = infer_signature(X_train, model.predict(X_train))

        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            input_example=X_train.head(1),
            signature=signature,
        )

        # -------- ONNX 저장 --------
        try:
            # 입력 feature 차원 (3축 15개)
            feature_dim = X_train.shape[1]
            initial_type = [("input", FloatTensorType([None, feature_dim]))]

            # ZipMap 끄기 → 확률을 plain float 텐서로 출력
            options = {id(model): {"zipmap": False}}

            onnx_model = convert_sklearn(
                model,
                initial_types=initial_type,
                options=options,
            )

            onnx_dir = os.getenv("ONNX_EXPORT_DIR", "/tmp/onnx_models")
            os.makedirs(onnx_dir, exist_ok=True)

            active_run = mlflow.active_run()
            run_id = active_run.info.run_id if active_run is not None else "no_runid"
            onnx_filename = f"fault_model_{run_id}.onnx"
            onnx_path = os.path.join(onnx_dir, onnx_filename)

            with open(onnx_path, "wb") as f:
                f.write(onnx_model.SerializeToString())

            print(f"[TRAIN-FAULT] ONNX model saved to {onnx_path}")

            # MLflow artifact 로도 같이 남겨두기
            mlflow.log_artifact(onnx_path, artifact_path="onnx")

            realtime_dir = os.getenv("REALTIME_MODEL_DIR")
            if realtime_dir:
                os.makedirs(realtime_dir, exist_ok=True)

                # 항상 고정 파일명으로 복사 → C#은 이 파일만 보면 됨
                realtime_onnx_path = os.path.join(realtime_dir, "fault_model_latest.onnx")
                shutil.copy2(onnx_path, realtime_onnx_path)

                # (선택) 메타 정보 JSON도 같이 떨궈주면 C#에서 표시 가능
                meta = {
                    "run_id": run_id,
                    "task_type": "fault_diagnosis",
                    "model_type": model_type,
                    "train_accuracy": float(train_acc),
                    "val_accuracy": float(val_acc) if val_acc is not None else None,
                    "test_accuracy": float(test_acc) if test_acc is not None else None,
                    "macro_f1": float(macro_f1) if macro_f1 is not None else None,
                }
                meta["class_labels"] = list(model.classes_)
                meta_path = os.path.join(realtime_dir, "fault_latest_meta.json")
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False, indent=2)

                print(f"[TRAIN-FAULT] realtime model updated: {realtime_onnx_path}")
                print(f"[TRAIN-FAULT] realtime meta saved: {meta_path}")
            else:
                print("[TRAIN-FAULT] REALTIME_MODEL_DIR not set. skip realtime deploy.")

        except Exception as ex:
            print(f"[TRAIN-FAULT][WARN] ONNX export failed: {ex}")

        print("[TRAIN-FAULT] training done.")
        print(
            f"[TRAIN-FAULT] model_type={model_type}, "
            f"train_acc={train_acc}, val_acc={val_acc}, test_acc={test_acc}"
        )

    # run 종료 후 세션 상태 업데이트
    mark_sessions_trained(used_session_ids)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--model-type",
        dest="model_type",
        default="rf",
        help="모델 타입 (예: rf, random_forest). 지금은 rf만 지원.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    train_and_log(model_type=args.model_type)
