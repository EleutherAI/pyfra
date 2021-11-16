from pyfra import *
from pathlib import Path 
import pyfra.contrib.web as web

@always_rerun()
def tpu_vm_create(rem_gcp, tpu_name, zone="europe-west4-a", type="v3-8"):
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

def kube_sh(pod, cmd, executable="bash", quiet=False):
    """
    Run a command in a kube pod
    """
    if executable == "bash":
        cmd = f"kubectl exec -it {pod} -- /bin/bash -c {quote(cmd)}"
    elif executable == "sh":
        cmd = f"kubectl exec -it {pod} -- /bin/sh -c {quote(cmd)}"
    elif executable == None:
        cmd = f"kubectl exec -it {pod} -- {quote(cmd)}"
    else:
        raise ValueError(f"executable must be bash or None, not {executable}")
    return local.sh(cmd, quiet=quiet)


def kube_copy_ssh_key(pod: str, key_path: str = None, quiet: bool = False):
    """
    Copy an ssh key to the k8 pod
    """
    if key_path is None:
        for pubkey in (Path(local.home()) / ".ssh").glob("*.pub"):
            kube_copy_ssh_key(pod, pubkey)
        return
    kube_sh(
        pod,
        f"echo {quote(local.path(key_path).read().strip())} >> ~/.ssh/authorized_keys",
        quiet=quiet,
    )


def kube_remote(
    pod: str, ssh_key_path: str = None, user=None, service_name=None, quiet=False
) -> Remote:
    """
    Get a remote object for a k8 pod
    """
    if service_name is None:
        service_name = pod.split("-")[0] + "-service"
    get_ip_cmd = f"kubectl get service/{service_name} -o jsonpath='{{.status.loadBalancer.ingress[0].ip}}'"
    ip = local.sh(get_ip_cmd, quiet=quiet).strip()
    if user is not None:
        ip = f"{user}@{ip}"

    # try to connect
    try:
        r = Remote(ip)
        r.sh(f"echo hello from {pod}", quiet=quiet)
        return r
    except ShellException:
        pass

    # copy ssh key
    kube_copy_ssh_key(pod, ssh_key_path, quiet=quiet)

    return Remote(ip)
