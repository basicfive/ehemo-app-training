import os
from pydantic import BaseModel

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class TrainingConfig(BaseModel):
    # base
    project_path: str = ROOT_DIR
    sd_scripts_path: str = os.path.join(ROOT_DIR, "sd-scripts")
    config_file_path: str = os.path.join(ROOT_DIR, "hairbe", "train_config.toml")
    subprocess_log_dir_path: str = os.path.join(ROOT_DIR, "hairbe", "log")
    gpu_id: str = "0"

    # model
    # base_model_path: str = os.path.join(ROOT_DIR, "hairbe", "models", "flux1-dev.safetensors")
    # t5xxl_path: str = os.path.join(ROOT_DIR, "hairbe", "models", "t5xxl_fp16.safetensors")
    # clip_path: str = os.path.join(ROOT_DIR, "hairbe", "models", "clip_l.safetensors")
    # vae_path: str = os.path.join(ROOT_DIR, "hairbe", "models", "ae.safetensors")

    base_model_path: str = "/home/hwichanjeon/ehemo/stable-diffusion-webui-forge/models/Stable-diffusion/flux1-dev-fp8.safetensors"
    t5xxl_path: str = "/home/hwichanjeon/ehemo/stable-diffusion-webui-forge/models/VAE/t5xxl_fp16.safetensors"
    clip_path: str = "/home/hwichanjeon/ehemo/stable-diffusion-webui-forge/models/VAE/clip_l.safetensors"
    vae_path: str = "/home/hwichanjeon/ehemo/stable-diffusion-webui-forge/models/VAE/ae.safetensors"


    # train
    train_dir_path: str = os.path.join(ROOT_DIR, "hairbe", "train")
    train_data_dir_path: str = os.path.join(ROOT_DIR, "hairbe", "train", "img")
    train_logging_dir: str = os.path.join(ROOT_DIR, "hairbe", "train", "log")
    output_dir_path: str = os.path.join(ROOT_DIR, "hairbe", "train", "model")
    sample_prompt_file_path: str = os.path.join(ROOT_DIR, "hairbe", "train", "model", "sample", "prompt.txt")


training_config = TrainingConfig()