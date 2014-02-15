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
import netifaces
import os
import subprocess
import re
import stat
import sys
import yaml

from fabric.api import env, execute, hide, parallel, put, run, settings, sudo
from netifaces import interfaces, ifaddresses, AF_INET

RING_TYPES = ['account', 'container', 'object']


def _fab_get_disk_wwn(disk):
    with hide('running', 'stdout', 'stderr'):
        output = sudo('/usr/local/bin/raidtool wwn %s' % (disk), shell=False)
        return output


def get_disk_wwn(ip, disk):
    with hide('running', 'stdout', 'stderr'):
        out = execute(_fab_get_disk_wwn, disk, hosts=[ip])
        return out[ip]


def _fab_get_disk_size(disk):
    with settings(warn_only=True):
        with hide('running', 'stdout', 'stderr'):
            output = sudo('/sbin/fdisk -l %s' % (disk), pty=False,
                          combine_stderr=False, shell=False)
            match = re.match(r'^Disk.*: ([\d\.]+) GB.*bytes', output)
            size = match.group(1)
            size = int(float(size))
            return size


def get_disk_size(ip, disk):
    with hide('running', 'stdout', 'stderr'):
        out = execute(_fab_get_disk_size, disk, hosts=[ip])
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

    def generate_commands(self, outdir, rebalance=True):
        commands = []

        for ringtype in RING_TYPES:
            commands.append(self._ring_create_command(ringtype, outdir))
            for zone, nodes in self.zones.iteritems():
                for node, disks in nodes.iteritems():
                    for disk in disks['disks']:
                        match = re.match('(.*)\d+$', disk)
                        blockdev = '/dev/%s' % match.group(1)
                        wwn = get_disk_wwn(node, blockdev)
                        weight = get_disk_size(node, blockdev)
                        cmd = self._ring_add_command(ringtype, outdir, zone,
                                                     node,
                                                     self.ports[ringtype],
                                                     disk, wwn, weight)
                        commands.append(cmd)
            if rebalance:
                commands.append(self._ring_rebalance_command(ringtype, outdir))

        return commands

    def generate_script(self, outdir, name='ring_builder.sh', rebalance=True):
        commands = ["#!/bin/bash\n"]
        commands = commands + self.generate_commands(outdir)

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


def bootstrap(args):
    if not os.path.exists(args.config):
        raise Exception("Could not find confguration file '%s'" % args.config)

    try:
        config = yaml.load(open(args.config, 'r'))
        ringsdef = SwiftRingsDefinition(config)

        if not os.path.exists(args.outdir):
            os.makedirs(args.outdir)

        build_script = ringsdef.generate_script(args.outdir)
        subprocess.call(build_script)

        myips = ip4_addresses()
        for node in ringsdef.nodes:
            if node not in myips:
                print "call rsync %s" % node
                dirname = os.path.dirname(args.outdir)
                subprocess.call('rsync -az %s %s:%s' % (args.outdir, node,
                                                        dirname), shell=True)
    except Exception as e:
        print >> sys.stderr, "There was an error bootrapping: '%s'" % e


def main():
    parser = argparse.ArgumentParser(description='Tool to modify swift config')
    subparsers = parser.add_subparsers()

    parser.add_argument('-i', dest='keyfile')
    parser.add_argument('-u', dest='user')

    parser_genconfig = subparsers.add_parser('bootstrap')
    parser_genconfig.add_argument('--config', required=True)
    parser_genconfig.add_argument('--outdir', required=True)
    parser_genconfig.set_defaults(func=bootstrap)

    args = parser.parse_args()
    if args.keyfile:
        env.key_filename = args.keyfile
    if args.user:
        env.user = args.user

    args.func(args)


if __name__ == '__main__':
    main()
