#!/usr/bin/env python2.7
import subprocess
import sys
import unittest


class TestRNASeqPipeline(unittest.TestCase):

    def test_docker_call(self):
        # print sys.argv
        tool = ['quay.io/ucsc_cgl/rnaseq-cgl-pipeline:{}'.format(tag)]
        base = ['docker', 'run']
        args = ['--star=/foo', '--rsem=/foo', '--kallisto=/foo', '--samples=/foo']
        sock = ['-v', '/var/run/docker.sock:/var/run/docker.sock']
        mirror = ['-v', '/foo:/foo']
        sample = ['-v', '/bar:/samples']
        inputs = ['-v', '/foobar:/inputs']
        # Check base call for help menu
        out = check_docker_output(command=base + tool, assert_fail=False)
        self.assertTrue('Please see the complete documentation' in out)


def check_docker_output(command, assert_fail=True):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = process.communicate()
    if assert_fail:
        assert process.returncode == 1
    else:
        assert process.returncode == 0
    return output[0]


if __name__ == '__main__':
    tag = sys.argv[1]
    del sys.argv[1]

    unittest.main()
