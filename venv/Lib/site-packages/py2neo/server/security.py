#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

from collections import namedtuple
from datetime import datetime, timedelta
from logging import getLogger
from os import makedirs, path
from socket import gethostname
from uuid import uuid4

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from six import u


log = getLogger(__name__)


Auth = namedtuple("Auth", ["user", "password"])


def make_auth(value=None, default_user=None, default_password=None):
    try:
        user, _, password = str(value or "").partition(":")
    except AttributeError:
        raise ValueError("Invalid auth string {!r}".format(value))
    else:
        return Auth(user or default_user or "neo4j",
                    password or default_password or uuid4().hex)


def make_self_signed_certificate():

    # create a private key
    log.info("Generating private key")
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )

    # Create a self-signed cert.
    log.info("Generating self-signed certificate")
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, u"GB"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"Kent"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, u"Canterbury"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"Example"),
        x509.NameAttribute(NameOID.COMMON_NAME, u(gethostname())),
    ])
    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.utcnow()
    ).not_valid_after(
        datetime.utcnow() + timedelta(days=7)
    ).add_extension(
        x509.SubjectAlternativeName([x509.DNSName(u"localhost")]),
        critical=False,
    ).sign(key, hashes.SHA256(), default_backend())

    return cert, key


def install_certificate(cert, name, *cert_dirs):
    for cert_dir in cert_dirs:
        try:
            makedirs(cert_dir)
        except OSError:
            pass
        cert_file = path.join(cert_dir, name)
        log.info("Installing certificate to %r", cert_file)
        with open(cert_file, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))


def install_private_key(key, name, *key_dirs):
    for key_dir in key_dirs:
        try:
            makedirs(key_dir)
        except OSError:
            pass
        key_file = path.join(key_dir, name)
        log.info("Installing private key to %r", key_file)
        with open(key_file, "wb") as f:
            f.write(key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            ))
