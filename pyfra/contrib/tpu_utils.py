from pyfra import *
import time


def make_tpu(rem, tpu_name, zone="europe-west4-a", project="youdreamof-1543654322305", tf_version="2.4.0", accelerator_type="v3", size=256):
    rem.sh("pip3 install tpunicorn")

    def tpus_by_state(state):
        return rem.sh("pu list").split('\n')[1:] >> filt(lambda x: state in x) >> each(columns, lambda x: x[4]) >> do(listify)

    live_tpus = tpus_by_state("READY")
    print("live tpus:", live_tpus)

    # check if tpu actually exists
    while tpu not in live_tpus:
        creating_tpus = tpus_by_state("CREATING")
        if tpu in creating_tpus: 
            print("tpu creating, waiting for status to change")
            time.sleep(30)
            live_tpus = tpus_by_state("READY")
            print("live tpus:", live_tpus)

            continue
        
        print("The following errors are expected, don't panic!")

        # this call will fail if the tpu doesn't already exist
        rem.sh(f"pu recreate {tpu} --yes --retry 3600 --retry-randomness 1.5", ignore_errors=True)

        # this call will fail if the tpu already exists and the previous call succeeded in making a tpu
        rem.sh(f"gcloud compute tpus create {tpu} --zone {zone} --project {project} --network default --version {tf_version} --accelerator-type {accelerator_type}-{size} --preemptible", ignore_errors=True)

        live_tpus = tpus_by_state("READY")
        print("live tpus:", live_tpus)
