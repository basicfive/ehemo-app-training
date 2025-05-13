import os
from pydantic import BaseModel

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class TrainingConfig(BaseModel):
    # base
    project_path: str = ROOT_DIR
    sd_scripts_path: str = os.path.join(ROOT_DIR, "sd-scripts")
    config_file_path: str = os.path.join(ROOT_DIR, "ehemo", "train_config.toml")
    subprocess_log_dir_path: str = os.path.join(ROOT_DIR, "ehemo", "log")
    gpu_id: str = "0"

    # model
    # base_model_path: str = os.path.join(ROOT_DIR, "ehemo", "models", "flux1-dev.safetensors")
    # t5xxl_path: str = os.path.join(ROOT_DIR, "ehemo", "models", "t5xxl_fp16.safetensors")
    # clip_path: str = os.path.join(ROOT_DIR, "ehemo", "models", "clip_l.safetensors")
    # vae_path: str = os.path.join(ROOT_DIR, "ehemo", "models", "ae.safetensors")

    base_model_path: str = "/home/hwichanjeon/models/checkpoints/flux1-dev-fp8.safetensors"
    t5xxl_path: str = "/home/hwichanjeon/models/vae/t5xxl_fp16.safetensors"
    clip_path: str = "/home/hwichanjeon/models/vae/clip_l.safetensors"
    vae_path: str = "/home/hwichanjeon/models/vae/ae.safetensors"

    # train
    train_dir_path: str = os.path.join(ROOT_DIR, "ehemo", "train")
    train_data_dir_path: str = os.path.join(ROOT_DIR, "ehemo", "train", "img")
    train_logging_dir: str = os.path.join(ROOT_DIR, "ehemo", "train", "log")
    output_dir_path: str = os.path.join(ROOT_DIR, "ehemo", "train", "model")
    sample_prompt_file_path: str = os.path.join(ROOT_DIR, "ehemo", "train", "model", "sample", "prompt.txt")
    total_steps: int = 2000


training_config = TrainingConfig()