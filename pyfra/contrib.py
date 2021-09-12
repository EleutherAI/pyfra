from pyfra import *

@force_run()
def make_tpu_vm(rem_gcp, tpu_name, zone="europe-west4-a", type="v3-8"):
    user = rem_gcp.sh("echo $USER").strip()

    def _get_tpu_ssh():
        ip = rem_gcp.sh(f"gcloud alpha compute tpus tpu-vm describe {tpu_name} --format='get(networkEndpoints[0].accessConfig.externalIp)'".strip())
        return Remote(f"{user}@{ip}")
    
    try:
        r = _get_tpu_ssh()
        r.sh("echo hello from tpu")
        return r
    except ShellException:
        pass

    rem_gcp.sh(f"""
    echo y | gcloud alpha compute tpus tpu-vm delete {tpu_name}
    gcloud alpha compute tpus tpu-vm create {tpu_name} \
        --zone={zone} \
        --accelerator-type={type} \
        --version=v2-alpha 
    gcloud alpha compute tpus tpu-vm ssh {tpu_name} --zone {zone} --command="echo $(cat {local.path("~/.ssh/id_rsa.pub")}) >> ~/.ssh/authorized_keys"
    """)

    time.sleep(10)

    return _get_tpu_ssh()
