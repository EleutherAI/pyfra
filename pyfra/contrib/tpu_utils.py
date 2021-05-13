from pyfra import *
import time
import json
from parse import parse

def make_tpu(rem, tpu_name, zone="europe-west4-a", project="youdreamof-1543654322305", tf_version="2.4.0", accelerator_type="v3", size=256):
    rem.sh("pu --help || pip3 install tpunicorn")

    def tpus_by_state(state):
        return rem.sh("pu list").split('\n')[1:] >> filt(lambda x: state in x) >> each(columns, lambda x: x[4]) >> do(listify)

    live_tpus = tpus_by_state("READY")
    print("live tpus:", live_tpus)

    # check if tpu actually exists
    while tpu_name not in live_tpus:
        creating_tpus = tpus_by_state("CREATING")
        if tpu_name in creating_tpus: 
            print("tpu creating, waiting for status to change")
            time.sleep(30)
            live_tpus = tpus_by_state("READY")
            print("live tpus:", live_tpus)

            continue
        
        print("The following errors are expected, don't panic!")

        # this call will fail if the tpu doesn't already exist
        rem.sh(f"pu recreate {tpu_name} --yes --retry 3600 --retry-randomness 1.5", ignore_errors=True)

        # this call will fail if the tpu already exists and the previous call succeeded in making a tpu
        rem.sh(f"gcloud compute tpus create {tpu_name} --zone {zone} --project {project} --network default --version {tf_version} --accelerator-type {accelerator_type}-{size} --preemptible", ignore_errors=True)

        live_tpus = tpus_by_state("READY")
        print("live tpus:", live_tpus)


def config_for(experiment_name, model_size, tpu_size, custom_config, models_bucket):
    conf = curl("https://gist.githubusercontent.com/leogao2/a5b53c1ef45e9be167cc7ccbfca7cabc/raw/c4ed65951fa3c941672ee4fd40f8c875aeebd487/run_config_1.3B.json") \
        >> do(json.loads)
    conf['model_path'] = f"{models_bucket}/{experiment_name}"
    conf['datasets'][0][0] = f"{experiment_name}_data"
    
    # set batch dim in mesh
    mesh = conf['mesh_shape'].split(',') >> each(lambda x: x.split(":"))
    mesh = {k: int(v) for k, v in mesh}

    total_size = 1
    for v in mesh.values():
        total_size *= v
    
    mesh['bat'] = (mesh['bat'] * tpu_size) // total_size
    conf['mesh_shape'] = ','.join(mesh.items() >> each(lambda x: f'{x[0]}:{x[1]}'))

    if model_size == '125M':
        model_conf = {
            "n_layer": 12,
            "n_embd": 768,
            "n_head": 12,
            "train_batch_size": 256,
            "lr": 6e-4,
        }
    elif model_size == '350M':
        model_conf = {
            "n_layer": 24,
            "n_embd": 1024,
            "n_head": 16,
            "train_batch_size": 256,
            "lr": 3e-4,
        }
    elif model_size == '760M':
        model_conf = {
            "n_layer": 24,
            "n_embd": 1536,
            "n_head": 16,
            "train_batch_size": 256,
            "lr": 2.5e-4,
        }
    elif model_size == '1.3B':
        model_conf = {
            "n_layer": 24,
            "n_embd": 2048,
            "n_head": 32,
            "train_batch_size": 512,
            "lr": 2e-4,
        }
    elif model_size == '2.7B':
        model_conf = {
            "n_layer": 32,
            "n_embd": 2560,
            "n_head": 32,
            "train_batch_size": 512,
            "lr": 1.6e-4,
        }
    else:
        raise NotImplementedError

    model_conf["attention_types"] = [[["global"], model_conf["n_layer"]]]

    for k, v in model_conf.items():
        conf[k] = v
    for k, v in custom_config.items():
        conf[k] = v

    return conf


def trim_slash(x):
    if x is None: return x
    if x[-1] == '/': return x
    return x


def train_model(rem, experiment_name, dataset_bucket, tpu_config={}, model_size="1.3B", models_bucket="gs://neo-models", resume_from=None, config={}): 
    dataset_bucket, models_bucket, resume_from = map(trim_slash, [dataset_bucket, models_bucket, resume_from])
    
    rem.sh(f"cd ~; git clone https://github.com/leogao2/gpt-neo/ ~/neo_{experiment_name} || cd ~/neo_{experiment_name} && git pull", ignore_errors=True)
    rem = rem.cd(f"~/neo_{experiment_name}")
    rem.sh("pip3 install -r requirements.txt")

    tpu_name = "neo_" + experiment_name
    tpu_size = tpu_config.get("size", 256)

    data_conf = curl("https://gist.githubusercontent.com/leogao2/a5b53c1ef45e9be167cc7ccbfca7cabc/raw/031b6bb9853d79ec2192a3648022185e3ce2e65d/dataset_config.json") \
        >> do(json.loads)
    data_conf["path"] = f"{dataset_bucket}/*.tfrecords"
    rem.jwrite(f"configs/{experiment_name}.json", config_for(experiment_name, model_size, tpu_size, config, models_bucket))
    rem.jwrite(f"configs/dataset_configs/{experiment_name}_data.json", data_conf)

    # if resuming, copy from old dir
    if resume_from is not None:
        index = latest_model_index(rem, resume_from)

        original = rem.sh(f"gsutil ls {resume_from}/*{index}*").strip().split("\n") >> filt(lambda x: x.startswith("gs://")) >> do(listify)
        target = original \
            >> each(lambda x: f"{models_bucket}/{experiment_name}/" + x.replace(f"ckpt-{index}", "ckpt-0").split("/")[-1]) >> do(listify)
        cmd = " & ".join(
            zip(original, target) >> each(lambda x: f"gsutil cp {x[0]} {x[1]}")
        ) + " & wait"
        print(cmd)
        rem.sh(cmd)

        # write checkpoint file
        rem.sh(f"echo 0 > checkpoint; gsutil cp checkpoint {models_bucket}/{experiment_name}/; rm checkpoint")

    make_tpu(rem, tpu_name, **tpu_config)

    rem.sh(f"python3 run_experiment.py --experiment_name {experiment_name} --tpu {tpu_name} --model {experiment_name} --json_save eval_{experiment_name}.jsonl --steps_per_checkpoint 1000")

    rsync(rem.file(f'eval_{experiment_name}.jsonl'), '.')


def latest_model_index(rem, model_path):
    files = rem.sh(f"gsutil ls {model_path}")

    latest = [
        parse(model_path + "/model.ckpt-{}.meta", f)
        for f in files.split('\n')
    ] >> filt(identity) >> each(lambda x: x[0], int) >> do(sorted, list, lambda x: x[-1])

    print("Latest checkpoint:", latest)

    return latest

