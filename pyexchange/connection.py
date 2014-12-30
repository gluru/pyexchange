"""
(c) 2013 LinkedIn Corp. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");?you may not use this file except in compliance with the License. You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software?distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""
import requests
from requests_ntlm import HttpNtlmAuth

import logging

from .exceptions import FailedExchangeException, OauthAuthException

log = logging.getLogger('pyexchange')


class ExchangeBaseConnection(object):
  """ Base class for Exchange connections."""

  def send(self, body, headers=None, retries=2, timeout=30, encoding="utf-8"):
    raise NotImplementedError


class ExchangeNTLMAuthConnection(ExchangeBaseConnection):
  """ Connection to Exchange that uses NTLM authentication """

  def __init__(self, url, username, password, **kwargs):
    self.url = url
    self.username = username
    self.password = password

    self.handler = None
    self.session = None
    self.password_manager = None

  def build_password_manager(self):
    if self.password_manager:
      return self.password_manager

    log.debug(u'Constructing password manager')

    self.password_manager = HttpNtlmAuth(self.username, self.password)

    return self.password_manager

  def build_session(self):
    if self.session:
      return self.session

    log.debug(u'Constructing opener')

    self.password_manager = self.build_password_manager()

    self.session = requests.Session()
    self.session.auth = self.password_manager

    return self.session

  def send(self, body, headers=None, retries=2, timeout=30, encoding=u"utf-8"):
    if not self.session:
      self.session = self.build_session()

    try:
      response = self.session.post(self.url, data=body, headers=headers)
      response.raise_for_status()
    except requests.exceptions.RequestException as err:
      log.debug(err.response.content)
      raise FailedExchangeException(u'Unable to connect to Exchange: %s' % err)

    log.info(u'Got response: {code}'.format(code=response.status_code))
    log.debug(u'Got response headers: {headers}'.format(headers=response.headers))
    log.debug(u'Got body: {body}'.format(body=response.text))

    return response.text



class ExchangeRequestsOauth(object):
  """
  This is the exchange oauth connection that adds the headers
  to the requests in order to oauth the requests
  """

  def __init__(self, access_token):
    """
    Inits the connection object
    :param access_token:  the access token if available
    """
    self._access_token = access_token


  def __call__(self, r):
    """
    It implements the Auth Manager interface from the requests library

    :param r: request object
    :return: the request object back
    """
    if not self._access_token:
      raise OauthAuthException("Access token not supplied")

    r.headers['Authorization']  = " ".join(["Bearer", self._access_token])
    return r



class ExchangeOauthConnection(ExchangeBaseConnection):
  """ Connection to Exchange that uses OAUTH authentication """

  def __init__(self, url, access_token, **kwargs):
    self.url = url
    self._access_token = access_token

    self.handler = None
    self.session = None
    self.auth_manager = None

  def build_auth_manager(self):
    if self.auth_manager:
      return self.auth_manager

    log.debug(u'Constructing auth manager')

    self.auth_manager = ExchangeRequestsOauth(self._access_token)

    return self.auth_manager

  def build_session(self):
    if self.session:
      return self.session

    log.debug(u'Constructing opener')

    self.auth_manager = self.build_auth_manager()

    self.session = requests.Session()
    self.session.auth = self.auth_manager

    return self.session

  def send(self, body, headers=None, retries=2, timeout=30, encoding=u"utf-8"):
    if not self.session:
      self.session = self.build_session()

    try:
      response = self.session.post(self.url, data=body, headers=headers)
      response.raise_for_status()
    except requests.exceptions.RequestException as err:
      log.debug(err.response.content)
      raise FailedExchangeException(u'Unable to connect to Exchange: %s' % err)

    log.info(u'Got response: {code}'.format(code=response.status_code))
    log.debug(u'Got response headers: {headers}'.format(headers=response.headers))
    log.debug(u'Got body: {body}'.format(body=response.text))

    return response.text
