# if you're using this file as an example of pyfra code, please do note that this is using fairly low level pyfra constructs

from pyfra import *
import time
import json
from parse import parse
import os


@once
def split_data(rem, input, shuffle=False):
    copy(input, rem.path("split_inp"), into=False)
    if shuffle:
        rem.sh(f"""
        pip install lm_dataformat
        rm -rf output_split
        wget -c https://gist.githubusercontent.com/leogao2/a81146bd01d7f1e4a65aca347047d3f9/raw/421de00fb5d3586c06be3f5660dfa2978846a2b2/split_lmd_shuffle.py

        python split_lmd_shuffle.py split_inp
        """)
    else:
        rem.sh(f"""
        pip install lm_dataformat
        rm -rf output_split
        wget -c https://gist.githubusercontent.com/leogao2/a81146bd01d7f1e4a65aca347047d3f9/raw/fe667a416f7c5860b610a90f824c72b2ff5c66c9/split_lmd.py
        python split_lmd.py split_inp
        """)

    return rem.path("output_split")

@once(v=1)
def tokenize(rem_tok, rem_gcp, input, dataset_name, dataset_bucket):
    if isinstance(dataset_bucket, str): dataset_bucket = [dataset_bucket]
    for bucket in dataset_bucket: rem_gcp.sh(f"gsutil -m rm -r {trim_slash(bucket)}/{dataset_name}", ignore_errors=True)

    env = rem_tok.env("tokenization_pyfra", "https://github.com/EleutherAI/gpt-neo")
    copy(input, env.path(f"inpdata"), into=False)

    env.sh("rm inpdata/current_chunk_incomplete", ignore_errors=True)

    cores = env.sh("nproc").split("\n")[-1].strip() >> do(int)
    procs = min(cores, len(env.ls("inpdata")))

    print(f"Using {procs} of {cores} cores")

    env.sh(f"""
    cd data
    [ -d {dataset_name}_tfrecords ] || TOKENIZERS_PARALLELISM=false python3 create_tfrecords.py --input_dir ../inpdata --name {dataset_name} --output_dir {dataset_name}_tfrecords --processes {procs}
    """)

    rem_gcp.sh(f"rm -rf {dataset_name}_tfrecords; mkdir {dataset_name}_tfrecords", ignore_errors=True)
    copy(env.path(f"data/{dataset_name}_tfrecords/"), rem_gcp.path(f"{dataset_name}_tfrecords"))
    for bucket in dataset_bucket: rem_gcp.sh(f"gsutil -m cp -r {dataset_name}_tfrecords {trim_slash(bucket)}/{dataset_name}")
    rem_gcp.rm(f"{dataset_name}_tfrecords")


def make_tpu(rem, tpu_name, zone="europe-west4-a", project="youdreamof-1543654322305", tf_version="2.4.0", accelerator_type="v3", size=256):
    rem.sh("pu --help || pip3 install tpunicorn")

    def tpus_by_state(state):
        return rem.sh("pu list").split('\n')[1:] >> filt(lambda x: state in x and f"{accelerator_type}-{size}" in x) >> each(columns, lambda x: x[4]) >> do(listify)

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
        rem.sh(f"pu delete {tpu_name} --yes", ignore_errors=True)

        # this call will fail if the tpu already exists
        rem.sh(f"gcloud compute tpus create {tpu_name} --zone {zone} --project {project} --network default --version {tf_version} --accelerator-type {accelerator_type}-{size} --preemptible", ignore_errors=True)

        live_tpus = tpus_by_state("READY")
        print("live tpus:", live_tpus)


def config_for(experiment_name, model_size, tpu_size, custom_config, model_bucket):
    conf = curl("https://gist.githubusercontent.com/leogao2/a5b53c1ef45e9be167cc7ccbfca7cabc/raw/c4ed65951fa3c941672ee4fd40f8c875aeebd487/run_config_1.3B.json") \
        >> do(json.loads)
    conf['model_path'] = model_bucket
    conf['datasets'][0][0] = f"{experiment_name}_data"

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
            "mesh_shape" : "bat:64,y:2",
        }
    elif model_size == '2.7B':
        model_conf = {
            "n_layer": 32,
            "n_embd": 2560,
            "n_head": 32,
            "train_batch_size": 512,
            "lr": 1.6e-4,
            "mesh_shape" : "bat:64,y:4",
        }
    else:
        raise NotImplementedError

    model_conf["attention_types"] = [[["global"], model_conf["n_layer"]]]

    for k, v in model_conf.items():
        conf[k] = v
    
    # set batch dim in mesh
    mesh = conf['mesh_shape'].split(',') >> each(lambda x: x.split(":"))
    mesh = {k: int(v) for k, v in mesh}

    total_size = 1
    for v in mesh.values():
        total_size *= v
    
    mesh['bat'] = (mesh['bat'] * tpu_size) // total_size
    conf['mesh_shape'] = ','.join(mesh.items() >> each(lambda x: f'{x[0]}:{x[1]}'))

    for k, v in custom_config.items():
        conf[k] = v

    return conf


def trim_slash(x):
    if x is None: return x
    if x[-1] == '/': return x[:-1]
    return x


@once
def train_model(rem, experiment_name, dataset_bucket, model_bucket, tpu_config={}, model_size="1.3B", val_data="gs://neo-d/atasets/pile_val.tfrecords", resume_from=None, config_override={}, tpu_name=None, steps_per_checkpoint=1000, git_repo="https://github.com/leogao2/gpt-neo/", git_branch=None): 
    dataset_bucket, model_bucket, resume_from = map(trim_slash, [dataset_bucket, model_bucket, resume_from])
    
    rem = rem.env(f"neo_{experiment_name}", git_repo, branch=git_branch)

    if tpu_name is None:
        tpu_name = "neo_" + experiment_name
    tpu_size = tpu_config.get("size", 256)

    data_conf = curl("https://gist.githubusercontent.com/leogao2/a5b53c1ef45e9be167cc7ccbfca7cabc/raw/031b6bb9853d79ec2192a3648022185e3ce2e65d/dataset_config.json") \
        >> do(json.loads)
    data_conf["path"] = f"{dataset_bucket}/*.tfrecords"
    data_conf["eval_path"] = val_data
    config_json = config_for(experiment_name, model_size, tpu_size, config_override, model_bucket)
    rem.path(f"configs/{experiment_name}.json").jwrite(config_json)
    rem.path(f"configs/dataset_configs/{experiment_name}_data.json").jwrite(data_conf)
    # if resuming, copy from old dir
    if resume_from is not None:
        try:
            train_index = latest_model_index(rem, model_bucket)
        except IndexError:
            train_index = 0
        if train_index == 0:
            resume_index = latest_model_index(rem, resume_from)
            reset_index = False # reset index doesn't work yet because tf is cursed

            original = rem.sh(f"gsutil ls {resume_from}/*{resume_index}*").strip().split("\n") >> filt(lambda x: x.startswith("gs://")) >> do(listify)
            target = original \
                >> each(lambda x: f"{model_bucket}/" + (x.replace(f"ckpt-{resume_index}", "ckpt-0") if reset_index else x).split("/")[-1]) >> do(listify)
            cmd = " & ".join(
                zip(original, target) >> each(lambda x: f"gsutil cp {x[0]} {x[1]}")
            ) + " & wait"
            print(cmd)
            rem.sh(cmd)

            if reset_index:
                ckpt_file = 'model_checkpoint_path: "model.ckpt-0"\nall_model_checkpoint_paths: "model.ckpt-0"'
                # write checkpoint file
                rem.sh(f"echo {ckpt_file | quote} > checkpoint; gsutil cp checkpoint {model_bucket}/; rm checkpoint")
            else:
                rem.sh(f"gsutil cp {resume_from}/checkpoint {model_bucket}/")

    make_tpu(rem, tpu_name, **tpu_config)

    rem.sh(f"python3 run_experiment.py --experiment_name {experiment_name} --tpu {tpu_name} --model {experiment_name} --json_save eval_{experiment_name}.jsonl --steps_per_checkpoint {steps_per_checkpoint} --opt_init_step --initial_heartbeat_timeout 28800")

    copy(rem.path(f'eval_{experiment_name}.jsonl'), '.')
    return config_json


@once
def train_model_jax(rem, experiment_name, tpu_name, region, custom_config): 
    env = rem.env(f"jax_{experiment_name}", "https://github.com/kingoflolz/mesh-transformer-jax/")

    wget("https://gist.githubusercontent.com/leogao2/bf311d064af8e7cd7b6c522e9835b577/raw/6ebab4465f9df30d2398ee21728a4196ac6da3a7/jax_config.json")
    conf = local.jread("jax_config.json")
    for k, v in custom_config.items():
        conf[k] = v
    env.path(f"configs/{experiment_name}.json").jwrite(conf)
    env.sh(f"python train.py --tpu {tpu_name} --preemptible --config configs/{experiment_name}.json --tpu_region {region}")

def latest_model_index(rem, model_path):
    files = rem.sh(f"gsutil ls {model_path}", ignore_errors=True)

    if 'CommandException: One or more URLs matched no objects.' in files:
        return 0

    latest = [
        parse(trim_slash(model_path) + "/model.ckpt-{}.meta", f)
        for f in files.split('\n')
    ] >> filt(identity) >> each(lambda x: x[0], int) >> do(sorted, list, ic, lambda x: x[-1])

    print("Latest checkpoint:", latest)

    return latest


def slugify(x):
    for p in r'/!?.~':
        x = x.replace(p, '_')
    
    return x


def convert_neo_to_hf(rem_gcp, rem_hf, model_path, hf_url, config):
    latest = latest_model_index(rem_gcp, model_path)

    return convert_neo_to_hf_for_index(rem_gcp, rem_hf, model_path, latest, hf_url, config)


@once
def convert_neo_to_hf_for_index(rem_gcp, rem_hf, model_path, latest, hf_url, config):
    assert 'HF_USER' in os.environ and 'HF_PWD' in os.environ

    rem_hf.path("hf_login").write(f"""
    spawn transformers-cli login
    expect "Username:"
    send "{os.environ['HF_USER']}\n"
    expect "Password:"
    send "{os.environ['HF_PWD']}\n"
    expect "Login successful"
    exit 0
    """)
    rem_hf.sh(f"""
    sudo apt install git-lfs expect -y
    pip3 install transformers torch
    expect ./hf_login ; rm hf_login
    transformers-cli repo ls-files || exit 1
    """)

    env_gcp = rem_gcp.env(slugify(hf_url))
    env_gcp.sh(f"""
    # rm -rf ~/.cache/huggingface/
    
    # copying model files
    mkdir -p model
    [ -f model/model.ckpt-{latest}.meta ] || gsutil -m cp {model_path}/*{latest}* model/
    gsutil cp {model_path}/checkpoint model/
    """).split('\n')

    env_gcp.path("config.json").jwrite(config)

    env_gcp.sh(f"""
    pip install git+https://github.com/leogao2/transformers@patch-3 torch tensorflow
    wget -c https://raw.githubusercontent.com/huggingface/transformers/master/src/transformers/models/gpt_neo/convert_gpt_neo_mesh_tf_to_pytorch.py
    
    mkdir output
    [ -f output/pytorch_model.bin ] || python3 convert_gpt_neo_mesh_tf_to_pytorch.py --tf_checkpoint_path model --config_file config.json --pytorch_dump_path output
    """)

    rem_hf.sh(f"mkdir -p converted/")
    copy(env_gcp.path(f"output/"), rem_hf.path(f"converted/{slugify(hf_url)}_tmp/"), symlink_ok=False)

    rem_gcp.rm(slugify(hf_url))

    org, repo = hf_url.split("/")
    rem_hf.sh(f"""
    transformers-cli repo create {repo} --organization {org} -y
    cd converted/
    git clone https://{os.environ['HF_USER']}:{os.environ['HF_PWD']}@huggingface.co/{hf_url} {slugify(hf_url)}
    cd {slugify(hf_url)}

    git lfs install
    git checkout -b main
    transformers-cli lfs-enable-largefiles .
    mv ../{slugify(hf_url)}_tmp/* .
    wget -c https://huggingface.co/EleutherAI/gpt-neo-125M/raw/main/merges.txt
    wget -c https://huggingface.co/EleutherAI/gpt-neo-125M/raw/main/special_tokens_map.json
    wget -c https://huggingface.co/EleutherAI/gpt-neo-125M/raw/main/tokenizer_config.json
    wget -c https://huggingface.co/EleutherAI/gpt-neo-125M/raw/main/vocab.json

    git add .
    git commit -am "Add model"
    git push origin main
    rm -rf ../{slugify(hf_url)}_tmp
    """)


def run_eval_harness(rem, tasks, model_name, batch_size=1, gpu_id=0, k=0):
    if model_name in ["ada", "babbage", "curie", "davinci", "curie-instruct-beta", "davinci-instruct-beta"]:
        model = "gpt3"
    else:
        model = "gpt2"

    tasks = ",".join(tasks)

    env = rem.env("eval_harness", "https://github.com/EleutherAI/lm-evaluation-harness/")
    env.sh("""
    pip3 install -U transformers sacrebleu
    """)
    env.sh("pip3 install torch==1.7.1+cu110  -f https://download.pytorch.org/whl/torch_stable.html")


    env.sh(f"CUDA_VISIBLE_DEVICES={gpu_id} python3 main.py --model {model} --model_args pretrained={model_name} --tasks {tasks} --output_path eval_{slugify(model_name)}.json --batch_size {batch_size}" + (f" --num_fewshot {k}" if k != 0 else ""))

    return env.path(f"eval_{slugify(model_name)}.json")


TASKS_LLONLY_SMALL = [
    "lambada",
    "piqa",
    "hellaswag",
    "winogrande",
    "mathqa",
    "pubmedqa",
    "boolq",
    "anli_r3",
    "openbookqa",
    "sciq",
    "cb",
    "copa",
    "multirc",
]

TASKS_HASTRAIN = [
    'anli_r1',
    'anli_r2',
    'anli_r3',
    'arc_challenge',
    'arc_easy',
    'boolq',
    'cb',
    'cola',
    'copa',
    'coqa',
    'drop',
    'ethics_cm',
    'ethics_deontology',
    'ethics_justice',
    'ethics_utilitarianism',
    'ethics_virtue',
    'headqa',
    'hellaswag',
    'hendrycksTest-abstract_algebra',
    'hendrycksTest-anatomy',
    'hendrycksTest-astronomy',
    'hendrycksTest-business_ethics',
    'hendrycksTest-clinical_knowledge',
    'hendrycksTest-college_biology',
    'hendrycksTest-college_chemistry',
    'hendrycksTest-college_computer_science',
    'hendrycksTest-college_mathematics',
    'hendrycksTest-college_medicine',
    'hendrycksTest-college_physics',
    'hendrycksTest-computer_security',
    'hendrycksTest-conceptual_physics',
    'hendrycksTest-econometrics',
    'hendrycksTest-electrical_engineering',
    'hendrycksTest-elementary_mathematics',
    'hendrycksTest-formal_logic',
    'hendrycksTest-global_facts',
    'hendrycksTest-high_school_biology',
    'hendrycksTest-high_school_chemistry',
    'hendrycksTest-high_school_computer_science',
    'hendrycksTest-high_school_european_history',
    'hendrycksTest-high_school_geography',
    'hendrycksTest-high_school_government_and_politics',
    'hendrycksTest-high_school_macroeconomics',
    'hendrycksTest-high_school_mathematics',
    'hendrycksTest-high_school_microeconomics',
    'hendrycksTest-high_school_physics',
    'hendrycksTest-high_school_psychology',
    'hendrycksTest-high_school_statistics',
    'hendrycksTest-high_school_us_history',
    'hendrycksTest-high_school_world_history',
    'hendrycksTest-human_aging',
    'hendrycksTest-human_sexuality',
    'hendrycksTest-international_law',
    'hendrycksTest-jurisprudence',
    'hendrycksTest-logical_fallacies',
    'hendrycksTest-machine_learning',
    'hendrycksTest-management',
    'hendrycksTest-marketing',
    'hendrycksTest-medical_genetics',
    'hendrycksTest-miscellaneous',
    'hendrycksTest-moral_disputes',
    'hendrycksTest-moral_scenarios',
    'hendrycksTest-nutrition',
    'hendrycksTest-philosophy',
    'hendrycksTest-prehistory',
    'hendrycksTest-professional_accounting',
    'hendrycksTest-professional_law',
    'hendrycksTest-professional_medicine',
    'hendrycksTest-professional_psychology',
    'hendrycksTest-public_relations',
    'hendrycksTest-security_studies',
    'hendrycksTest-sociology',
    'hendrycksTest-us_foreign_policy',
    'hendrycksTest-virology',
    'hendrycksTest-world_religions',
    'logiqa',
    'math_algebra',
    'math_counting_and_prob',
    'math_geometry',
    'math_intermediate_algebra',
    'math_num_theory',
    'math_prealgebra',
    'math_precalc',
    'mathqa',
    'mnli',
    'mnli_mismatched',
    'mrpc',
    'multirc',
    'openbookqa',
    'piqa',
    'qnli',
    'qqp',
    'race',
    'record',
    'rte',
    'sciq',
    'squad2',
    'sst',
    'webqs',
    'wic',
    'winogrande',
    'wnli',
    'wsc',
]
