#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2014, Blue Box Group, Inc.
# Copyright (c) 2014, Craig Tracey <craigtracey@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#

import argparse
import json
import netifaces
import os
import subprocess
import re
import shutil
import stat
import sys
import tempfile
import yaml

from fabric.api import env, execute, hide, parallel, put, run, settings, sudo
from netifaces import interfaces, ifaddresses, AF_INET

RING_TYPES = ['account', 'container', 'object']

_host_lshw_output = {}


def _parse_lshw_output(output, blockdev):
    disks = re.split('\s*\*', output.strip())
    alldisks = []
    for disk in disks:
        d = {}
        for line in disk.split('\n'):
            match = re.match('^-(\w+)', line)
            if match:
                d['class'] = match.group(1)
            else:
                match = re.match('^\s+([\w\s]+):\s+(.*)$', line)
                if match:
                    key = re.sub('\s', '_', match.group(1))
                    val = match.group(2)
                    d[key] = val
        if 'class' in d:
            alldisks.append(d)

    for d in alldisks:
        if d['logical_name'] == blockdev:
            serial = d['serial']
            match = re.match('\s*(\d+)[MG]iB.*', d['size'])
            if not match:
                raise Exception("Could not find size of disk %s" % disk)
            size = int(match.group(1))
            return size, serial


def _fab_get_disk_size_serial(ip, blockdev):
    with hide('running', 'stdout', 'stderr'):
        global _host_lshw_output
        output = None
        if ip in _host_lshw_output:
            output = _host_lshw_output[ip]
        else:
            output = sudo('lshw -C disk', pty=False, shell=False)
            _host_lshw_output[ip] = output
        return _parse_lshw_output(output, blockdev)


def get_disk_size_serial(ip, blockdev):
    with hide('running', 'stdout', 'stderr'):
        out = execute(_fab_get_disk_size_serial, ip, blockdev, hosts=[ip])
        return out[ip]


class SwiftRingsDefinition(object):

    def __init__(self, data=None):
        self.ring_builder_cmd = "swift-ring-builder"
        self.ports = {
            'object': 6000,
            'container': 6001,
            'account': 6002,
        }
        self.replicas = 3
        self.min_part_hours = 1
        self.zones = {}
        if data:
            self.__dict__.update(data)

    def __repr__(self):
        return str(self.__dict__)

    def _ring_create_command(self, ringtype, outdir):
        return "%s %s/%s.builder create %d %d %d" % (
            self.ring_builder_cmd, outdir, ringtype, int(self.part_power),
            int(self.replicas), int(self.min_part_hours))

    def _ring_add_command(self, ringtype, outdir, zone, host, port, disk,
                          metadata, weight):
        return "%s %s/%s.builder add %s-%s:%d/%s_%s %d" % (
            self.ring_builder_cmd, outdir, ringtype, zone, host, int(port),
            disk, metadata, int(weight))

    def _ring_rebalance_command(self, ringtype, outdir):
        return "%s %s/%s.builder rebalance" % (
            self.ring_builder_cmd, outdir, ringtype)

    @property
    def nodes(self):
        ret = set()
        if self.zones and isinstance(self.zones, dict):
            for zone, nodes in self.zones.iteritems():
                ret.update(nodes.keys())
        return ret

    def generate_commands(self, outdir, rebalance=True, meta=None):
        commands = []

        for ringtype in RING_TYPES:
            commands.append(self._ring_create_command(ringtype, outdir))
            for zone, nodes in self.zones.iteritems():
                for node, disks in nodes.iteritems():
                    for disk in disks['disks']:
                        match = re.match('(.*)\d+$', disk)
                        blockdev = '/dev/%s' % match.group(1)

                        # treat size as weight and serial as metadata
                        weight, serial = get_disk_size_serial(node, blockdev)

                        metadata = meta
                        if not meta:
                            metadata = serial
                        cmd = self._ring_add_command(ringtype, outdir, zone,
                                                     node,
                                                     self.ports[ringtype],
                                                     disk, metadata, weight)
                        commands.append(cmd)
            if rebalance:
                commands.append(self._ring_rebalance_command(ringtype, outdir))

        return commands

    def generate_script(self, outdir, name='ring_builder.sh', rebalance=True,
                        meta=None):
        commands = ["#!/bin/bash\n"]
        commands = commands + self.generate_commands(outdir, rebalance,
                                                     meta)

        outfile = os.path.join(outdir, name)
        f = open(outfile, 'w')
        for command in commands:
            f.write("%s\n" % command)
        f.close()

        st = os.stat(outfile)
        os.chmod(outfile, st.st_mode | stat.S_IEXEC)
        return outfile


def ip4_addresses():
    ips = []
    for interface in interfaces():
        addresses = ifaddresses(interface)
        if addresses and AF_INET in addresses:
            for link in addresses[AF_INET]:
                ips.append(link['addr'])
    return ips


@parallel
def _fab_copy_swift_directory(local_files, remote_dir):
    put(local_files, remote_dir, mirror_local_mode=True)


def bootstrap(args):
    rc = 0
    if not os.path.exists(args.config):
        raise Exception("Could not find confguration file '%s'" % args.config)

    try:
        config = yaml.load(open(args.config, 'r'))
        ringsdef = SwiftRingsDefinition(config)

        tempdir = tempfile.mkdtemp()
        build_script = ringsdef.generate_script(tempdir, meta=args.meta)
        subprocess.call(build_script)

        tempfiles = os.path.join(tempdir, "*")
        execute(_fab_copy_swift_directory, tempfiles, args.outdir,
                hosts=ringsdef.nodes)
    except Exception as e:
        print >> sys.stderr, "There was an error bootrapping: '%s'" % e
        rc = -1

#    shutil.rmtree(tempdir)
    sys.exit(rc)


def main():
    parser = argparse.ArgumentParser(description='Tool to modify swift config')
    subparsers = parser.add_subparsers()

    parser.add_argument('-i', dest='keyfile')
    parser.add_argument('-u', dest='user')

    parser_genconfig = subparsers.add_parser('bootstrap')
    parser_genconfig.add_argument('--config', required=True)
    parser_genconfig.add_argument('--outdir', required=True)
    parser_genconfig.add_argument('--meta', default=None)
    parser_genconfig.set_defaults(func=bootstrap)

    args = parser.parse_args()
    if args.keyfile:
        env.key_filename = args.keyfile
    if args.user:
        env.user = args.user

    args.func(args)


if __name__ == '__main__':
    main()
