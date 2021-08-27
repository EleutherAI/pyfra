from pyfra import *
import pyfra.remote as pyr
import os
import random


def setup_remote(ind):
    """ setup any state specific to the execution of the given module."""
    sh(f"""
    # docker kill pyfra_test_remote_{ind}
    # docker container rm pyfra_test_remote_{ind}
    cd tests
    cp ~/.ssh/id_rsa.pub .
    docker build -t pyfra_test_remote .
    docker run --rm --name pyfra_test_remote_{ind} -d pyfra_test_remote
    true
    """)

    ip = sh("docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' pyfra_test_remote_" + str(ind))
    rem = Remote("root@"+ip)
    rem.sh("rm -rf pyfra_envs")
    return rem


def setup_module(module):
    global rem1, rem2
    rem1 = setup_remote(1)
    rem2 = setup_remote(2)

def test_workdir_semantics():
    global rem1, rem2

    assert pyr._normalize_homedir("somepath") == "~/somepath"
    assert pyr._normalize_homedir("~/somepath") == "~/somepath"
    assert pyr._normalize_homedir(".") == "~"
    assert pyr._normalize_homedir("~") == "~"
    assert pyr._normalize_homedir("/") == "/"
    assert pyr._normalize_homedir("somepath/") == "~/somepath"
    assert pyr._normalize_homedir("/somepath/") == "/somepath"
    assert pyr._normalize_homedir("./somepath") == "~/somepath"
    
    for rem in [rem1, rem2]:
        assert rem.path("somepath").fname == "~/somepath"
        assert rem.path("~/somepath").fname == "~/somepath"
        assert rem.path(".").fname == "~"
        assert rem.path("~").fname == "~"
        assert rem.path("/").fname == "/"
        assert rem.path("somepath/").fname == "~/somepath"
        assert rem.path("/somepath/").fname == "/somepath"
        assert rem.path("./somepath").fname == "~/somepath"
    
    assert local.path("somepath").fname == os.getcwd() + "/somepath"
    assert local.path("~/somepath").fname == "~/somepath"
    assert local.path(".").fname == os.getcwd()
    assert local.path("~").fname == "~"
    assert local.path("/").fname == "/"
    assert local.path("somepath/").fname == os.getcwd() + "/somepath"
    assert local.path("/somepath/").fname == "/somepath"
    assert local.path("./somepath").fname == os.getcwd() + "/somepath"

    # sh path
    assert sh("echo $PWD") == os.getcwd()
    assert local.sh("echo $PWD") == os.getcwd()
    assert rem1.sh("echo $PWD") == "/root"

    # env path
    env1 = rem1.env("env1")
    env2 = rem2.env("env2")

    locenv1 = local.env("locenv1")
    locenv2 = local.env("locenv2")

    def copy_path_test(a, b):
        payload = random.randint(0, 99999)
        a.sh(f"mkdir origin_test_dir; echo hello world {payload} > origin_test_dir/test_pyfra.txt", ignore_errors=True)
        b.sh("mkdir test_dir_1", ignore_errors=True)
        b.sh("mkdir test_dir_2", ignore_errors=True)

        # check right into=False behavior
        copy(a.path("origin_test_dir/test_pyfra.txt"), b.path("test2_pyfra.txt"))
        copy(a.path("origin_test_dir"), b.path("test_dir_1"))
        copy(a.path("origin_test_dir"), b.path("test_dir_2"), into=False)
        ic(b.sh("ls"))
        assert b.sh("cat test2_pyfra.txt") == f"hello world {payload}"
        assert b.sh("cat test_dir_1/origin_test_dir/test_pyfra.txt") == f"hello world {payload}"
        assert b.sh("cat test_dir_2/test_pyfra.txt") == f"hello world {payload}"
        a.sh("rm -rf origin_test_dir")
        b.sh("rm -rf test2_pyfra.txt test_dir_1 test_dir_2")

    ## env to env
    copy_path_test(env1, env2)

    ## env to rem
    copy_path_test(env1, rem2)

    ## rem to env
    copy_path_test(rem1, env2)

    ## rem to rem
    copy_path_test(rem1, rem2)

    ## same rem to rem
    copy_path_test(rem1, rem1)

    ## same rem to env
    copy_path_test(rem1, env1)

    ## same env to rem
    copy_path_test(env1, rem1)

    ## same env to rem
    copy_path_test(env1, env1)

    ## local rem to local rem
    copy_path_test(local, local)

    ## local rem to local env
    copy_path_test(local, locenv2)

    ## local env to local rem
    copy_path_test(locenv1, local)

    ## local env to local rem
    copy_path_test(locenv1, locenv2)

    # local no rem to rem
    sh("echo test 123 > test_pyfra.txt")
    copy("test_pyfra.txt", rem1.path("test2_pyfra.txt"))
    assert rem1.sh("cat test2_pyfra.txt") == f"test 123"
    sh("rm test_pyfra.txt")
    rem1.sh("rm test2_pyfra.txt")
    
    # rem to local no rem
    rem1.sh("echo test 1234 > test_pyfra.txt")
    copy(rem1.path("test_pyfra.txt"), "test2_pyfra.txt")
    assert sh("cat test2_pyfra.txt") == f"test 1234"
    rem1.sh("rm test_pyfra.txt")
    sh("rm test2_pyfra.txt")

    # todo: test fread/fwrite


def test_fns():
    global rem1, rem2

    env1 = rem1.env("env1")
    env2 = rem2.env("env2")

    locenv1 = local.env("locenv1")
    locenv2 = local.env("locenv2")

    def fns_test(rem):
        rem.sh("rm -rf testing_dir_fns; mkdir testing_dir_fns && cd testing_dir_fns && touch a && touch b && touch c && echo $PWD")
        assert rem.ls("testing_dir_fns") == ['a', 'b', 'c']
        assert 'testing_dir_fns' in rem.ls()
        rem.rm("testing_dir_fns")

        assert 'testing_dir_fns' not in rem.ls()

        # make sure no error when deleting nonexistent
        rem.rm("testing_dir_fns")

        # check file read and write
        rem.path("testfile.pyfra").write("goose")
        assert rem.path("testfile.pyfra").read() == "goose"
        rem.path("testfile.pyfra").write("goose", append=True)
        assert rem.path("testfile.pyfra").read() == "goosegoose"
        rem.rm("testfile.pyfra")

    for rem in [local, rem1, rem2, env1, env2, locenv1, locenv2]: fns_test(rem)


def test_remotefile_implicit_copy():
    global rem1, rem2

    rem1.sh("rm *.pyfra", ignore_errors=True)
    rem2.sh("rm *.pyfra", ignore_errors=True)

    # implicit-read
    f1 = rem1.path("testfile_copy.pyfra")
    f1.write("goose")
    assert rem2.sh(f"cat {f1}") == "goose"

    # implicit-write
    f2 = rem2.path("testfile_copy2.pyfra")
    rem1.sh(f"echo geese > {f2}")
    assert rem2.sh(f"cat {f2}") == "geese"

    # implicit both read and write
    f3 = rem1.path("testfile_copy3.pyfra")
    f3.write("canada goose")
    f4 = rem1.path("testfile_copy4.pyfra")
    assert rem2.sh(f"cat {f3} | tr [a-z] [A-Z] | tee {f4}") == "CANADA GOOSE"
    assert rem2.sh(f"cat {f4}") == "CANADA GOOSE"


# todo: test env w git


def teardown_module(module):
    """ teardown any state that was previously setup with a setup_module
    method.
    """
    # sh("docker kill pyfra_test_remote_1")
    # sh("docker kill pyfra_test_remote_2")

