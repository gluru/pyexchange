"""
(c) 2013 LinkedIn Corp. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");?you may not use this file except in compliance with the License. You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software?distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import logging
from ..base.calendar import BaseExchangeCalendarEvent, BaseExchangeCalendarService, ExchangeEventOrganizer, ExchangeEventResponse
from ..base.folder import BaseExchangeFolder, BaseExchangeFolderService
from ..base.soap import ExchangeServiceSOAP
from ..base.email import BaseExchangeEmailItem, BaseExchangeEmailService, BaseExchangeAttachmentItem
from ..exceptions import FailedExchangeException, ExchangeStaleChangeKeyException, ExchangeItemNotFoundException, ExchangeInternalServerTransientErrorException, ExchangeIrresolvableConflictException, InvalidEventType

from . import soap_request

from lxml import etree
from copy import deepcopy
from datetime import date
import warnings

log = logging.getLogger("pyexchange")


class Exchange2010Service(ExchangeServiceSOAP):

  def calendar(self, id="calendar"):
    return Exchange2010CalendarService(service=self, calendar_id=id)

  def mail(self, folder_id="inbox"):
    return Exchange2010EmailService(service=self, folder_id=folder_id)

  def contacts(self):
    raise NotImplementedError("Sorry - nothin' here. Feel like adding it? :)")

  def folder(self):
    return Exchange2010FolderService(service=self)

  def _send_soap_request(self, body, headers=None, retries=2, timeout=30, encoding="utf-8"):
    headers = {
      "Accept": "text/xml",
      "Content-type": "text/xml; charset=%s " % encoding
    }
    return super(Exchange2010Service, self)._send_soap_request(body, headers=headers, retries=retries, timeout=timeout, encoding=encoding)

  def _check_for_errors(self, xml_tree):
    super(Exchange2010Service, self)._check_for_errors(xml_tree)
    self._check_for_exchange_fault(xml_tree)

  def _check_for_exchange_fault(self, xml_tree):

    # If the request succeeded, we should see a <m:ResponseCode>NoError</m:ResponseCode>
    # somewhere in the response. if we don't (a) see the tag or (b) it doesn't say "NoError"
    # then flip out

    response_codes = xml_tree.xpath(u'//m:ResponseCode', namespaces=soap_request.NAMESPACES)

    if not response_codes:
      raise FailedExchangeException(u"Exchange server did not return a status response", None)

    # The full (massive) list of possible return responses is here.
    # http://msdn.microsoft.com/en-us/library/aa580757(v=exchg.140).aspx
    for code in response_codes:
      if code.text == u"ErrorChangeKeyRequiredForWriteOperations":
        # change key is missing or stale. we can fix that, so throw a special error
        raise ExchangeStaleChangeKeyException(u"Exchange Fault (%s) from Exchange server" % code.text)
      elif code.text == u"ErrorItemNotFound":
        # exchange_invite_key wasn't found on the server
        raise ExchangeItemNotFoundException(u"Exchange Fault (%s) from Exchange server" % code.text)
      elif code.text == u"ErrorIrresolvableConflict":
        # tried to update an item with an old change key
        raise ExchangeIrresolvableConflictException(u"Exchange Fault (%s) from Exchange server" % code.text)
      elif code.text == u"ErrorInternalServerTransientError":
        # temporary internal server error. throw a special error so we can retry
        raise ExchangeInternalServerTransientErrorException(u"Exchange Fault (%s) from Exchange server" % code.text)
      elif code.text == u"ErrorCalendarOccurrenceIndexIsOutOfRecurrenceRange":
        # just means some or all of the requested instances are out of range
        pass
      elif code.text != u"NoError":
        raise FailedExchangeException(u"Exchange Fault (%s) from Exchange server" % code.text)


class Exchange2010EmailService(BaseExchangeEmailService):
  """
  The service implementation for email handling
  """

  def get_email(self, email_id):
    """
    Gets an exchange email item back
    """
    return Exchange2010EmailItem(self.service, id=email_id)


  def get_attachment(self, attachment_id):
    """
    This method does an api call and pulls the attachment body not only the meta
    """
    return Exchange2010AttachmentItem(self.service, id=attachment_id)



  def list_emails(self, per_page=10, offset=0, folder_id="inbox"):
    """
    Lists the emails from the specified folder
    """
    return Exchange2010EmailList(self.service,
                                folder_id=self.folder_id,
                                max_entries=per_page,
                                offset=offset)


  def get_inbox(self):
    """
    Getting the inbox_rules back
    """
    body = soap_request.get_folder(folder_id="inbox", format=u'IdOnly')
    response_xml = self.service.send(body)
    return response_xml


class Exchange2010EmailItem(BaseExchangeEmailItem):
  """
  The implementation of the ExchangeEmailItem
  """

  def _init_from_service(self, id):
    log.debug(u'Creating new Exchange2010EmailItem object from ID')
    body = soap_request.get_email(id)
    response_xml = self.service.send(body)
    properties = self._parse_response_for_get_email(response_xml)

    self.update_properties(properties)
    self._id = id
    log.debug(u'Created new event object with ID: %s' % self._id)

    return self


  def _init_from_xml(self, xml=None):
    log.debug(u'Creating new Exchange2010EmailItem object from XML')

    properties = self._parse_response_for_get_email(xml)
    self.update_properties(properties)
    self._id, self._change_key = self._parse_id_and_change_key_from_response(xml)

    log.debug(u'Created new email object with ID: %s' % self._id)

    return self


  def _parse_id_and_change_key_from_response(self, response):
    """
    Gets the id of the message and the change_key

    :param response: the xml reponse
    :return:
    """
    id_elements = response.xpath(u'//m:Items/t:Message/t:ItemId', namespaces=soap_request.NAMESPACES)

    if id_elements:
      id_element = id_elements[0]
      return id_element.get(u"Id", None), id_element.get(u"ChangeKey", None)
    else:
      return None, None



  def _parse_response_for_get_email(self, xml_resp):
    """
    Parses the soap email message and pulls the fields we need
    The default message format is something like this :
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Items>
            <t:Message>
              <t:ItemId Id="some-id"/>
              <t:Subject>Test From Denis</t:Subject>
              <t:Sensitivity>Normal</t:Sensitivity>
              <t:Body BodyType="HTML">BodyMsg</t:Body>
              <t:Size>15706</t:Size>
              <t:DateTimeSent>2014-12-23T12:51:13Z</t:DateTimeSent>
              <t:DateTimeCreated>2014-12-23T12:51:15Z</t:DateTimeCreated>
              <t:DateTimeReceived>2014-12-23T12:51:15Z</t:DateTimeReceived>
              <t:ResponseObjects>
                <t:ReplyToItem/>
                <t:ReplyAllToItem/>
                <t:ForwardItem/>
              </t:ResponseObjects>
              <t:HasAttachments>false</t:HasAttachments>
              <t:ToRecipients>
                <t:Mailbox>
                  <t:Name>Recipent1</t:Name>
                  <t:EmailAddress>recipent@example.com</t:EmailAddress>
                  <t:RoutingType>SMTP</t:RoutingType>
                </t:Mailbox>
              </t:ToRecipients>
              <t:IsReadReceiptRequested>false</t:IsReadReceiptRequested>
              <t:From>
                <t:Mailbox>
                  <t:Name>Sender</t:Name>
                  <t:EmailAddress>email@example.com</t:EmailAddress>
                  <t:RoutingType>SMTP</t:RoutingType>
                </t:Mailbox>
              </t:From>
              <t:IsRead>true</t:IsRead>
            </t:Message>
          </m:Items>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
    """
    property_map = {
      "subject":{
        "xpath": u'//m:Items/t:Message/t:Subject'
      },
      "body_html":{
        "xpath": u'//m:Items/t:Message/t:Body[@BodyType="HTML"]'
      },
      "size":{
        "xpath": u'//m:Items/t:Message/t:Size',
        "cast":u"int"
      },
      "sent_time":{
        "xpath": u'//m:Items/t:Message/t:DateTimeSent',
        "cast":u"datetime"
      },
      "created_time":{
        "xpath": u'//m:Items/t:Message/t:DateTimeCreated',
        "cast":u"datetime"
      },
      "received_time":{
        "xpath": u'//m:Items/t:Message/t:DateTimeReceived',
        "cast":u"datetime"
      },
      "has_attachments":{
        "xpath": u'//m:Items/t:Message/t:HasAttachments',
        "cast":u"bool"
      },
      "is_read":{
        "xpath": u'//m:Items/t:Message/t:IsRead',
        "cast":u"bool"
      }
    }

    result = self.service._xpath_to_dict(element=xml_resp, property_map=property_map, namespace_map=soap_request.NAMESPACES)

    #extract the sender from the xpath
    sender_dict = self._parse_email_sender(xml_resp)
    result["sender"] = sender_dict

    #extract the recipients from the message
    recipients_dict = self._parse_email_recipients(xml_resp)
    result["recipients"] = recipients_dict

    #Extract the cc list from it
    cc_dict = self._parse_email_cc_recipients(xml_resp)
    result["cc_recipients"] = cc_dict

    if result.get("has_attachments"):
      attachments = self._parse_email_attachments(xml_resp)
      result["attachments"] = attachments

    return result


  def _parse_mailbox_item(self, xml_resp):
    """
    Parses a mailbox item which is in the following format

    <t:Mailbox>
      <t:Name>Name Here</t:Name>
      <t:EmailAddress>EmailHere</t:EmailAddress>
      <t:RoutingType>SMTP</t:RoutingType>
    </t:Mailbox>
    """
    property_map = {
      u'name':
      {
        u'xpath': u't:Name'
      },
      u'email':
      {
        u'xpath': u't:EmailAddress'
      },
      u'routing_type':
      {
        u'xpath': u't:ResponseType'
      }
    }

    result = self.service._xpath_to_dict(element=xml_resp, property_map=property_map, namespace_map=soap_request.NAMESPACES)
    return result


  def _parse_email_sender(self, xml_resp):
    """
    Parses the email sender and gets back a dict of the fields
    """
    from_part = xml_resp.xpath(u'//m:Items/t:Message/t:From/t:Mailbox', namespaces=soap_request.NAMESPACES)
    return self._parse_mailbox_item(from_part[0])


  def _parse_email_recipients(self, xml_resp):
    """
    Parses the recipients the email was sent to
    """
    recipient_path = u'//m:Items/t:Message/t:ToRecipients/t:Mailbox'
    return self._parse_email_mailboxes(xml_resp, recipient_path)


  def _parse_email_cc_recipients(self, xml_resp):
    """
    Parses the cc parts of the message
    """
    recipient_path = u'//m:Items/t:Message/t:CcRecipients/t:Mailbox'
    return self._parse_email_mailboxes(xml_resp, recipient_path)



  def _parse_email_mailboxes(self, xml_resp, xml_path):
    """
    A more generic version of parsing the mailboxes
    """
    result = []
    mailbox_parts = xml_resp.xpath(xml_path, namespaces=soap_request.NAMESPACES)

    for mailbox in mailbox_parts:
      tmp_recipient = self._parse_mailbox_item(mailbox)
      result.append(tmp_recipient)

    return result

  def _parse_email_attachments(self, xml_resp):
    """
    Only interested in attachment ids
    <t:Attachments>
      <t:FileAttachment>
        <t:AttachmentId Id="some_id"/>
        <t:Name>attach_name</t:Name>
        <t:ContentType>application/octet-stream</t:ContentType>
        <t:ContentId>content_id</t:ContentId>
      </t:FileAttachment>
    </t:Attachments>
    """
    property_map = {
      u'name':
      {
        u'xpath': u't:Name'
      },
      u'content_type':
      {
        u'xpath': u't:ContentType'
      }
    }

    xml_path = u'//m:Items/t:Message/t:Attachments/t:FileAttachment'
    attachments = xml_resp.xpath(xml_path, namespaces=soap_request.NAMESPACES)


    if not attachments:
      return []

    results = []
    for attach in attachments:
      result = self.service._xpath_to_dict(element=attach,
                                           property_map=property_map,
                                           namespace_map=soap_request.NAMESPACES)

      attach_id_obj = attach.xpath(u't:AttachmentId', namespaces=soap_request.NAMESPACES)
      if not attach_id_obj:
        continue

      result["attachment_id"] = attach_id_obj[0].get("Id", None)
      results.append(result)

    return results


EMAIL_ITEM_DETAIL_ALL = "all"
EMAIL_ITEM_DETAIL_IDS = "ids"

class Exchange2010EmailList(object):
  """
  Creates and stores a list of Exchange2010EmailItem in the self.emails field
  """

  def __init__(self, service=None, max_entries=10, offset=0, folder_id="inbox", detail=EMAIL_ITEM_DETAIL_ALL):
    """
    :param service:
    :param max_entries:
    :param offset:
    :param folder_id:
    :param detail: It can be all|ids depending how much data we should fetch

    :return:
    """
    self.service = service
    self.count = 0
    self.emails = []
    self.email_ids = []
    self.detail = detail

    self.max_entries = max_entries
    self.offset = offset
    self.folder_id = folder_id

    body = soap_request.find_emails(folder_id=folder_id,
                                    max_per_page=self.max_entries,
                                    offset=self.offset)

    response_xml = self.service.send(body)
    #Loads the emails from the api
    self._parse_response_for_all_emails(response_xml)


  def _parse_response_for_all_emails(self, xml_resp):
    """
    Parses the response from find item

    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder IndexedPagingOffset="7" TotalItemsInView="7" IncludesLastItemInRange="true">
            <t:Items>
              <t:Message>
              </t:Message>

    """
    if self.detail == EMAIL_ITEM_DETAIL_IDS:
      items = xml_resp.xpath(u'//m:FindItemResponseMessage/m:RootFolder/t:Items/t:Message/t:ItemId', namespaces=soap_request.NAMESPACES)
    else:
      items = xml_resp.xpath(u'//m:FindItemResponseMessage/m:RootFolder/t:Items/t:Message', namespaces=soap_request.NAMESPACES)

    if not items:
      log.debug(u'No email items found with search parameters.')
      #There is no need tog o further on this
      return self

    self.count = len(items)
    log.debug(u'Found %s items' % self.count)

    for item in items:
      if self.detail == EMAIL_ITEM_DETAIL_IDS:
        item_id = item.get("Id", None)
        self.email_ids.append(item_id)
        self._add_email_from_id(item_id)
      else:
        self._add_email_from_xml(soap_request.M.Items(deepcopy(item)))

    return self


  def _add_email_from_id(self, item_id):
    """
    Adds an email by making a remote request
    """
    email_item = Exchange2010EmailItem(self.service, id=item_id)
    self.emails.append(email_item)


  def _add_email_from_xml(self, xml_resp):
    """
    You don't make another call just create one email item from xml request
    """
    email_item = Exchange2010EmailItem(self.service, xml=xml_resp)
    self.emails.append(email_item)
    self.email_ids.append(email_item.id)


class Exchange2010AttachmentItem(BaseExchangeAttachmentItem):
  """
  The implementation of the ExchangeEmailItem
  """

  def _init_from_service(self, id):
    log.debug(u'Creating new Exchange2010AttachmentItem object from ID')
    body = soap_request.get_attachment(id)
    response_xml = self.service.send(body)
    properties = self._parse_response_for_get_attachment(response_xml)

    self.update_properties(properties)
    self._id = id
    log.debug(u'Created new event object with ID: %s' % self._id)

    return self


  def _init_from_xml(self, xml=None):
    log.debug(u'Creating new Exchange2010EmailItem object from XML')

    properties = self._parse_response_for_get_attachment(xml)
    self.update_properties(properties)
    self._id = self._parse_id_and_change_key_from_response(xml)

    log.debug(u'Created new email object with ID: %s' % self._id)

    return self


  def _parse_id_and_change_key_from_response(self, response):
    """
    Gets the id of the message and the change_key

    :param response: the xml reponse
    :return:
    """
    id_elements = response.xpath(u'//m:Attachments/t:FileAttachment/t:AttachmentId', namespaces=soap_request.NAMESPACES)

    if id_elements:
      id_element = id_elements[0]
      return id_element.get(u"Id", None)

    return None



  def _parse_response_for_get_attachment(self, xml_resp):
    """
    Parses the attachment response back

    <m:GetAttachmentResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetAttachmentResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Attachments>
            <t:FileAttachment>
              <t:AttachmentId Id="the_id"/>
              <t:Name>messages.xsd</t:Name>
              <t:ContentType>application/octet-stream</t:ContentType>
              <t:ContentId>content_id</t:ContentId>
              <t:Content>
                The content of the attachment
              </t:Content>
            </t:FileAttachment>
          </m:Attachments>
        </m:GetAttachmentResponseMessage>
      </m:ResponseMessages>
    </m:GetAttachmentResponse>
    """
    property_map = {
      "name":{
        "xpath": u'//m:Attachments/t:FileAttachment/t:Name'
      },
      "content_type":{
        "xpath": u'//m:Attachments/t:FileAttachment/t:ContentType'
      },
      "content_id":{
        "xpath": u'//m:Attachments/t:FileAttachment/t:ContentId'
      },
      "content":{
        "xpath": u'//m:Attachments/t:FileAttachment/t:Content'
      }
    }

    result = self.service._xpath_to_dict(element=xml_resp, property_map=property_map, namespace_map=soap_request.NAMESPACES)

    return result




class Exchange2010CalendarService(BaseExchangeCalendarService):

  def event(self, id=None, **kwargs):
    return Exchange2010CalendarEvent(service=self.service, id=id, **kwargs)

  def get_event(self, id):
    return Exchange2010CalendarEvent(service=self.service, id=id)

  def new_event(self, **properties):
    return Exchange2010CalendarEvent(service=self.service, calendar_id=self.calendar_id, **properties)

  def list_events(self, start=None, end=None, details=False):
    return Exchange2010CalendarEventList(service=self.service, start=start, end=end, details=details)


class Exchange2010CalendarEventList(object):
  """
  Creates & Stores a list of Exchange2010CalendarEvent items in the "self.events" variable.
  """
  def __init__(self, service=None, start=None, end=None, details=False):
    self.service = service
    self.count = 0
    self.start = start
    self.end = end
    self.events = list()
    self.event_ids = list()
    self.details = details

    # This request uses a Calendar-specific query between two dates.
    body = soap_request.get_calendar_items(format=u'AllProperties', start=self.start, end=self.end)
    response_xml = self.service.send(body)
    self._parse_response_for_all_events(response_xml)

    # Populate the event ID list, for convenience reasons.
    for event in self.events:
      self.event_ids.append(event._id)

    # If we have requested all the details, basically repeat the previous 3 steps,
    # but instead of start/stop, we have a list of ID fields.
    if self.details:
      log.debug(u'Received request for all details, retrieving now!')
      self.load_all_details()

  def _parse_response_for_all_events(self, response):
    """
    This function will retrieve *most* of the event data, excluding Organizer & Attendee details
    """
    items = response.xpath(u'//m:FindItemResponseMessage/m:RootFolder/t:Items/t:CalendarItem', namespaces=soap_request.NAMESPACES)
    if not items:
      items = response.xpath(u'//m:GetItemResponseMessage/m:Items/t:CalendarItem', namespaces=soap_request.NAMESPACES)
    if items:
      self.count = len(items)
      log.debug(u'Found %s items' % self.count)

      for item in items:
        self._add_event(xml=soap_request.M.Items(deepcopy(item)))
    else:
      log.debug(u'No calendar items found with search parameters.')

    return self

  def _add_event(self, xml=None):
    log.debug(u'Adding new event to all events list.')
    event = Exchange2010CalendarEvent(service=self.service, xml=xml)
    log.debug(u'Subject of new event is %s' % event.subject)
    self.events.append(event)
    return self

  def load_all_details(self):
    """
    This function will execute all the event lookups for known events.

    This is intended for use when you want to have a completely populated event entry, including
    Organizer & Attendee details.
    """
    log.debug(u"Loading all details")
    if self.count > 0:
      # Now, empty out the events to prevent duplicates!
      del(self.events[:])

      # Send the SOAP request with the list of exchange ID values.
      log.debug(u"Requesting all event details for events: {event_list}".format(event_list=str(self.event_ids)))
      body = soap_request.get_item(exchange_id=self.event_ids, format=u'AllProperties')
      response_xml = self.service.send(body)

      # Re-parse the results for all the details!
      self._parse_response_for_all_events(response_xml)

    return self


class Exchange2010CalendarEvent(BaseExchangeCalendarEvent):

  def _init_from_service(self, id):
    log.debug(u'Creating new Exchange2010CalendarEvent object from ID')
    body = soap_request.get_item(exchange_id=id, format=u'AllProperties')
    response_xml = self.service.send(body)
    properties = self._parse_response_for_get_event(response_xml)

    self._update_properties(properties)
    self._id = id
    log.debug(u'Created new event object with ID: %s' % self._id)

    self._reset_dirty_attributes()

    return self

  def _init_from_xml(self, xml=None):
    log.debug(u'Creating new Exchange2010CalendarEvent object from XML')

    properties = self._parse_response_for_get_event(xml)
    self._update_properties(properties)
    self._id, self._change_key = self._parse_id_and_change_key_from_response(xml)

    log.debug(u'Created new event object with ID: %s' % self._id)
    self._reset_dirty_attributes()

    return self

  def as_json(self):
    raise NotImplementedError

  def validate(self):

    if self.recurrence is not None:

      if not (isinstance(self.recurrence_end_date, date)):
        raise ValueError('recurrence_end_date must be of type date')
      elif (self.recurrence_end_date < self.start.date()):
        raise ValueError('recurrence_end_date must be after start')

      if self.recurrence == u'daily':

        if not (isinstance(self.recurrence_interval, int) and 1 <= self.recurrence_interval <= 999):
          raise ValueError('recurrence_interval must be an int in the range from 1 to 999')

      elif self.recurrence == u'weekly':

        if not (isinstance(self.recurrence_interval, int) and 1 <= self.recurrence_interval <= 99):
          raise ValueError('recurrence_interval must be an int in the range from 1 to 99')

        if self.recurrence_days is None:
          raise ValueError('recurrence_days is required')
        for day in self.recurrence_days.split(' '):
          if day not in self.WEEKLY_DAYS:
            raise ValueError('recurrence_days received unknown value: %s' % day)

      elif self.recurrence == u'monthly':

        if not (isinstance(self.recurrence_interval, int) and 1 <= self.recurrence_interval <= 99):
          raise ValueError('recurrence_interval must be an int in the range from 1 to 99')

      elif self.recurrence == u'yearly':

        pass  # everything is pulled from start

      else:

        raise ValueError('recurrence received unknown value: %s' % self.recurrence)

    super(Exchange2010CalendarEvent, self).validate()

  def create(self):
    """
    Creates an event in Exchange. ::

        event = service.calendar().new_event(
          subject=u"80s Movie Night",
          location = u"My house",
        )
        event.create()

    Invitations to attendees are sent out immediately.

    """
    self.validate()
    body = soap_request.new_event(self)

    response_xml = self.service.send(body)
    self._id, self._change_key = self._parse_id_and_change_key_from_response(response_xml)

    return self

  def resend_invitations(self):
    """
    Resends invites for an event.  ::

        event = service.calendar().get_event(id='KEY HERE')
        event.resend_invitations()

    Anybody who has not declined this meeting will get a new invite.
    """

    if not self.id:
      raise TypeError(u"You can't send invites for an event that hasn't been created yet.")

    # Under the hood, this is just an .update() but with no attributes changed.
    # We're going to enforce that by checking if there are any changed attributes and bail if there are
    if self._dirty_attributes:
      raise ValueError(u"There are unsaved changes to this invite - please update it first: %r" % self._dirty_attributes)

    self.refresh_change_key()
    body = soap_request.update_item(self, [], calendar_item_update_operation_type=u'SendOnlyToAll')
    self.service.send(body)

    return self

  def update(self, calendar_item_update_operation_type=u'SendToAllAndSaveCopy', **kwargs):
    """
    Updates an event in Exchange.  ::

        event = service.calendar().get_event(id='KEY HERE')
        event.location = u'New location'
        event.update()

    If no changes to the event have been made, this method does nothing.

    Notification of the change event is sent to all users. If you wish to just notify people who were
    added, specify ``send_only_to_changed_attendees=True``.
    """
    if not self.id:
      raise TypeError(u"You can't update an event that hasn't been created yet.")

    if 'send_only_to_changed_attendees' in kwargs:
      warnings.warn(
        "The argument send_only_to_changed_attendees is deprecated.  Use calendar_item_update_operation_type instead.",
        DeprecationWarning,
      )  # 20140502
      if kwargs['send_only_to_changed_attendees']:
        calendar_item_update_operation_type = u'SendToChangedAndSaveCopy'

    VALID_UPDATE_OPERATION_TYPES = (
      u'SendToNone', u'SendOnlyToAll', u'SendOnlyToChanged',
      u'SendToAllAndSaveCopy', u'SendToChangedAndSaveCopy',
    )
    if calendar_item_update_operation_type not in VALID_UPDATE_OPERATION_TYPES:
      raise ValueError('calendar_item_update_operation_type has unknown value')

    self.validate()

    if self._dirty_attributes:
      log.debug(u"Updating these attributes: %r" % self._dirty_attributes)
      self.refresh_change_key()

      body = soap_request.update_item(self, self._dirty_attributes, calendar_item_update_operation_type=calendar_item_update_operation_type)
      self.service.send(body)
      self._reset_dirty_attributes()
    else:
      log.info(u"Update was called, but there's nothing to update. Doing nothing.")

    return self

  def cancel(self):
    """
    Cancels an event in Exchange.  ::

        event = service.calendar().get_event(id='KEY HERE')
        event.cancel()

    This will send notifications to anyone who has not declined the meeting.
    """
    if not self.id:
      raise TypeError(u"You can't delete an event that hasn't been created yet.")

    self.refresh_change_key()
    self.service.send(soap_request.delete_event(self))
    # TODO rsanders high - check return status to make sure it was actually sent
    return None

  def move_to(self, folder_id):
    """
    :param str folder_id: The Calendar ID to where you want to move the event to.
    Moves an event to a different folder (calendar).  ::

      event = service.calendar().get_event(id='KEY HERE')
      event.move_to(folder_id='NEW CALENDAR KEY HERE')
    """
    if not folder_id:
      raise TypeError(u"You can't move an event to a non-existant folder")

    if not isinstance(folder_id, basestring):
      raise TypeError(u"folder_id must be a string")

    if not self.id:
      raise TypeError(u"You can't move an event that hasn't been created yet.")

    self.refresh_change_key()
    response_xml = self.service.send(soap_request.move_event(self, folder_id))
    new_id, new_change_key = self._parse_id_and_change_key_from_response(response_xml)
    if not new_id:
      raise ValueError(u"MoveItem returned success but requested item not moved")

    self._id = new_id
    self._change_key = new_change_key
    self.calendar_id = folder_id
    return self

  def get_master(self):
    """
      get_master()
      :raises InvalidEventType: When this method is called on an event that is not a Occurrence type.

      This will return the master event to the occurrence.

      **Examples**::

        event = service.calendar().get_event(id='<event_id>')
        print event.type  # If it prints out 'Occurrence' then that means we could get the master.

        master = event.get_master()
        print master.type  # Will print out 'RecurringMaster'.


    """

    if self.type != 'Occurrence':
      raise InvalidEventType("get_master method can only be called on a 'Occurrence' event type")

    body = soap_request.get_master(exchange_id=self._id, format=u"AllProperties")
    response_xml = self.service.send(body)

    return Exchange2010CalendarEvent(service=self.service, xml=response_xml)

  def get_occurrence(self, instance_index):
    """
      get_occurrence(instance_index)
      :param iterable instance_index: This should be tuple or list of integers which correspond to occurrences.
      :raises TypeError: When instance_index is not an iterable of ints.
      :raises InvalidEventType: When this method is called on an event that is not a RecurringMaster type.

      This will return a list of occurrence events.

      **Examples**::

        master = service.calendar().get_event(id='<event_id>')

        # The following will return the first 20 occurrences in the recurrence.
        # If there are not 20 occurrences, it will only return what it finds.
        occurrences = master.get_occurrence(range(1,21))
        for occurrence in occurrences:
          print occurrence.start

    """

    if not all([isinstance(i, int) for i in instance_index]):
      raise TypeError("instance_index must be an interable of type int")

    if self.type != 'RecurringMaster':
      raise InvalidEventType("get_occurrance method can only be called on a 'RecurringMaster' event type")

    body = soap_request.get_occurrence(exchange_id=self._id, instance_index=instance_index, format=u"AllProperties")
    response_xml = self.service.send(body)

    items = response_xml.xpath(u'//m:GetItemResponseMessage/m:Items', namespaces=soap_request.NAMESPACES)
    events = []
    for item in items:
      event = Exchange2010CalendarEvent(service=self.service, xml=deepcopy(item))
      if event.id:
        events.append(event)

    return events

  def conflicting_events(self):
    """
      conflicting_events()

      This will return a list of conflicting events.

      **Example**::

        event = service.calendar().get_event(id='<event_id>')
        for conflict in event.conflicting_events():
          print conflict.subject

    """

    if not self.conflicting_event_ids:
      return []

    body = soap_request.get_item(exchange_id=self.conflicting_event_ids, format="AllProperties")
    response_xml = self.service.send(body)

    items = response_xml.xpath(u'//m:GetItemResponseMessage/m:Items', namespaces=soap_request.NAMESPACES)
    events = []
    for item in items:
      event = Exchange2010CalendarEvent(service=self.service, xml=deepcopy(item))
      if event.id:
        events.append(event)

    return events

  def refresh_change_key(self):

    body = soap_request.get_item(exchange_id=self._id, format=u"IdOnly")
    response_xml = self.service.send(body)
    self._id, self._change_key = self._parse_id_and_change_key_from_response(response_xml)

    return self

  def _parse_id_and_change_key_from_response(self, response):

    id_elements = response.xpath(u'//m:Items/t:CalendarItem/t:ItemId', namespaces=soap_request.NAMESPACES)

    if id_elements:
      id_element = id_elements[0]
      return id_element.get(u"Id", None), id_element.get(u"ChangeKey", None)
    else:
      return None, None

  def _parse_response_for_get_event(self, response):

    result = self._parse_event_properties(response)

    organizer_properties = self._parse_event_organizer(response)
    if organizer_properties is not None:
      if 'email' not in organizer_properties:
        organizer_properties['email'] = None
      result[u'organizer'] = ExchangeEventOrganizer(**organizer_properties)

    attendee_properties = self._parse_event_attendees(response)
    result[u'_attendees'] = self._build_resource_dictionary([ExchangeEventResponse(**attendee) for attendee in attendee_properties])

    resource_properties = self._parse_event_resources(response)
    result[u'_resources'] = self._build_resource_dictionary([ExchangeEventResponse(**resource) for resource in resource_properties])

    result['_conflicting_event_ids'] = self._parse_event_conflicts(response)

    return result

  def _parse_event_properties(self, response):

    property_map = {
      u'subject': {
        u'xpath': u'//m:Items/t:CalendarItem/t:Subject',
      },
      u'location':
      {
        u'xpath': u'//m:Items/t:CalendarItem/t:Location',
      },
      u'availability':
      {
        u'xpath': u'//m:Items/t:CalendarItem/t:LegacyFreeBusyStatus',
      },
      u'start':
      {
        u'xpath': u'//m:Items/t:CalendarItem/t:Start',
        u'cast': u'datetime',
      },
      u'end':
      {
        u'xpath': u'//m:Items/t:CalendarItem/t:End',
        u'cast': u'datetime',
      },
      u'html_body':
      {
        u'xpath': u'//m:Items/t:CalendarItem/t:Body[@BodyType="HTML"]',
      },
      u'text_body':
      {
        u'xpath': u'//m:Items/t:CalendarItem/t:Body[@BodyType="Text"]',
      },
      u'_type':
      {
        u'xpath': u'//m:Items/t:CalendarItem/t:CalendarItemType',
      },
      u'reminder_minutes_before_start':
      {
        u'xpath': u'//m:Items/t:CalendarItem/t:ReminderMinutesBeforeStart',
        u'cast': u'int',
      },
      u'is_all_day':
      {
        u'xpath': u'//m:Items/t:CalendarItem/t:IsAllDayEvent',
        u'cast': u'bool',
      },
      u'recurrence_end_date':
      {
        u'xpath': u'//m:Items/t:CalendarItem/t:Recurrence/t:EndDateRecurrence/t:EndDate',
        u'cast': u'date_only_naive',
      },
      u'recurrence_interval':
      {
        u'xpath': u'//m:Items/t:CalendarItem/t:Recurrence/*/t:Interval',
        u'cast': u'int',
      },
      u'recurrence_days':
      {
        u'xpath': u'//m:Items/t:CalendarItem/t:Recurrence/t:WeeklyRecurrence/t:DaysOfWeek',
      },
    }

    result = self.service._xpath_to_dict(element=response, property_map=property_map, namespace_map=soap_request.NAMESPACES)

    try:
      recurrence_node = response.xpath(u'//m:Items/t:CalendarItem/t:Recurrence', namespaces=soap_request.NAMESPACES)[0]
    except IndexError:
      recurrence_node = None

    if recurrence_node is not None:

      if recurrence_node.find('t:DailyRecurrence', namespaces=soap_request.NAMESPACES) is not None:
        result['recurrence'] = 'daily'

      elif recurrence_node.find('t:WeeklyRecurrence', namespaces=soap_request.NAMESPACES) is not None:
        result['recurrence'] = 'weekly'

      elif recurrence_node.find('t:AbsoluteMonthlyRecurrence', namespaces=soap_request.NAMESPACES) is not None:
        result['recurrence'] = 'monthly'

      elif recurrence_node.find('t:AbsoluteYearlyRecurrence', namespaces=soap_request.NAMESPACES) is not None:
        result['recurrence'] = 'yearly'

    return result

  def _parse_event_organizer(self, response):

    organizer = response.xpath(u'//m:Items/t:CalendarItem/t:Organizer/t:Mailbox', namespaces=soap_request.NAMESPACES)

    property_map = {
      u'name':
      {
        u'xpath': u't:Name'
      },
      u'email':
      {
        u'xpath': u't:EmailAddress'
      },
    }

    if organizer:
      return self.service._xpath_to_dict(element=organizer[0], property_map=property_map, namespace_map=soap_request.NAMESPACES)
    else:
      return None

  def _parse_event_resources(self, response):
    property_map = {
      u'name':
      {
        u'xpath': u't:Mailbox/t:Name'
      },
      u'email':
      {
        u'xpath': u't:Mailbox/t:EmailAddress'
      },
      u'response':
      {
        u'xpath': u't:ResponseType'
      },
      u'last_response':
      {
        u'xpath': u't:LastResponseTime',
        u'cast': u'datetime'
      },
    }

    result = []

    resources = response.xpath(u'//m:Items/t:CalendarItem/t:Resources/t:Attendee', namespaces=soap_request.NAMESPACES)

    for attendee in resources:
      attendee_properties = self.service._xpath_to_dict(element=attendee, property_map=property_map, namespace_map=soap_request.NAMESPACES)
      attendee_properties[u'required'] = True

      if u'last_response' not in attendee_properties:
        attendee_properties[u'last_response'] = None

      result.append(attendee_properties)

    return result

  def _parse_event_attendees(self, response):

    property_map = {
      u'name':
      {
        u'xpath': u't:Mailbox/t:Name'
      },
      u'email':
      {
        u'xpath': u't:Mailbox/t:EmailAddress'
      },
      u'response':
      {
        u'xpath': u't:ResponseType'
      },
      u'last_response':
      {
        u'xpath': u't:LastResponseTime',
        u'cast': u'datetime'
      },
    }

    result = []

    required_attendees = response.xpath(u'//m:Items/t:CalendarItem/t:RequiredAttendees/t:Attendee', namespaces=soap_request.NAMESPACES)
    for attendee in required_attendees:
      attendee_properties = self.service._xpath_to_dict(element=attendee, property_map=property_map, namespace_map=soap_request.NAMESPACES)
      attendee_properties[u'required'] = True

      if u'last_response' not in attendee_properties:
        attendee_properties[u'last_response'] = None

      result.append(attendee_properties)

    optional_attendees = response.xpath(u'//m:Items/t:CalendarItem/t:OptionalAttendees/t:Attendee', namespaces=soap_request.NAMESPACES)

    for attendee in optional_attendees:
      attendee_properties = self.service._xpath_to_dict(element=attendee, property_map=property_map, namespace_map=soap_request.NAMESPACES)
      attendee_properties[u'required'] = False

      if u'last_response' not in attendee_properties:
        attendee_properties[u'last_response'] = None

      result.append(attendee_properties)

    return result

  def _parse_event_conflicts(self, response):
    conflicting_ids = response.xpath(u'//m:Items/t:CalendarItem/t:ConflictingMeetings/t:CalendarItem/t:ItemId', namespaces=soap_request.NAMESPACES)
    return [id_element.get(u"Id") for id_element in conflicting_ids]


class Exchange2010FolderService(BaseExchangeFolderService):

  def folder(self, id=None, **kwargs):
    return Exchange2010Folder(service=self.service, id=id, **kwargs)

  def get_folder(self, id):
    """
      :param str id:  The Exchange ID of the folder to retrieve from the Exchange store.

      Retrieves the folder specified by the id, from the Exchange store.

      **Examples**::

        folder = service.folder().get_folder(id)

    """

    return Exchange2010Folder(service=self.service, id=id)

  def new_folder(self, **properties):
    """
      new_folder(display_name=display_name, folder_type=folder_type, parent_id=parent_id)
      :param str display_name:  The display name given to the new folder.
      :param str folder_type:  The type of folder to create.  Possible values are 'Folder',
        'CalendarFolder', 'ContactsFolder', 'SearchFolder', 'TasksFolder'.
      :param str parent_id:  The parent folder where the new folder will be created.

      Creates a new folder with the given properties.  Not saved until you call the create() method.

      **Examples**::

        folder = service.folder().new_folder(
          display_name=u"New Folder Name",
          folder_type="CalendarFolder",
          parent_id='calendar',
        )
        folder.create()

    """

    return Exchange2010Folder(service=self.service, **properties)

  def find_folder(self, parent_id):
    """
      find_folder(parent_id)
      :param str parent_id:  The parent folder to list.

      This method will return a list of sub-folders to a given parent folder.

      **Examples**::

        # Iterate through folders within the default 'calendar' folder.
        folders = service.folder().find_folder(parent_id='calendar')
        for folder in folders:
          print(folder.display_name)

        # Delete all folders within the 'calendar' folder.
        folders = service.folder().find_folder(parent_id='calendar')
        for folder in folders:
          folder.delete()
    """

    body = soap_request.find_folder(parent_id=parent_id, format=u'AllProperties')
    response_xml = self.service.send(body)
    return self._parse_response_for_find_folder(response_xml)

  def _parse_response_for_find_folder(self, response):

    result = []
    folders = response.xpath(u'//t:Folders/t:*', namespaces=soap_request.NAMESPACES)
    for folder in folders:
      result.append(
        Exchange2010Folder(
          service=self.service,
          xml=etree.fromstring(etree.tostring(folder))  # Might be a better way to do this
        )
      )

    return result


class Exchange2010Folder(BaseExchangeFolder):

  def _init_from_service(self, id):

    body = soap_request.get_folder(folder_id=id, format=u'AllProperties')
    response_xml = self.service.send(body)
    properties = self._parse_response_for_get_folder(response_xml)

    self._update_properties(properties)

    return self

  def _init_from_xml(self, xml):

    properties = self._parse_response_for_get_folder(xml)
    self._update_properties(properties)

    return self

  def create(self):
    """
    Creates a folder in Exchange. ::

      calendar = service.folder().new_folder(
        display_name=u"New Folder Name",
        folder_type="CalendarFolder",
        parent_id='calendar',
      )
      calendar.create()
    """

    self.validate()
    body = soap_request.new_folder(self)

    response_xml = self.service.send(body)
    self._id, self._change_key = self._parse_id_and_change_key_from_response(response_xml)

    return self

  def delete(self):
    """
    Deletes a folder from the Exchange store. ::

      folder = service.folder().get_folder(id)
      print("Deleting folder: %s" % folder.display_name)
      folder.delete()
    """

    if not self.id:
      raise TypeError(u"You can't delete a folder that hasn't been created yet.")

    body = soap_request.delete_folder(self)

    response_xml = self.service.send(body)  # noqa
    # TODO: verify deletion
    self._id = None
    self._change_key = None

    return None

  def move_to(self, folder_id):
    """
    :param str folder_id: The Folder ID of what will be the new parent folder, of this folder.
    Move folder to a different location, specified by folder_id::

      folder = service.folder().get_folder(id)
      folder.move_to(folder_id="ID of new location's folder")
    """

    if not folder_id:
      raise TypeError(u"You can't move to a non-existant folder")

    if not isinstance(folder_id, basestring):
      raise TypeError(u"folder_id must be a string")

    if not self.id:
      raise TypeError(u"You can't move a folder that hasn't been created yet.")

    response_xml = self.service.send(soap_request.move_folder(self, folder_id))  # noqa

    result_id, result_key = self._parse_id_and_change_key_from_response(response_xml)
    if self.id != result_id:
      raise ValueError(u"MoveFolder returned success but requested folder not moved")

    self.parent_id = folder_id
    return self

  def _parse_response_for_get_folder(self, response):
    FOLDER_PATH = u'//t:Folder | //t:CalendarFolder | //t:ContactsFolder | //t:SearchFolder | //t:TasksFolder'

    path = response.xpath(FOLDER_PATH, namespaces=soap_request.NAMESPACES)[0]
    result = self._parse_folder_properties(path)
    return result

  def _parse_folder_properties(self, response):

    property_map = {
      u'display_name': {u'xpath': u't:DisplayName'},
    }

    self._id, self._change_key = self._parse_id_and_change_key_from_response(response)
    self._parent_id = self._parse_parent_id_and_change_key_from_response(response)[0]
    self.folder_type = etree.QName(response).localname

    return self.service._xpath_to_dict(element=response, property_map=property_map, namespace_map=soap_request.NAMESPACES)

  def _parse_id_and_change_key_from_response(self, response):

    id_elements = response.xpath(u'//t:FolderId', namespaces=soap_request.NAMESPACES)

    if id_elements:
      id_element = id_elements[0]
      return id_element.get(u"Id", None), id_element.get(u"ChangeKey", None)
    else:
      return None, None

  def _parse_parent_id_and_change_key_from_response(self, response):

    id_elements = response.xpath(u'//t:ParentFolderId', namespaces=soap_request.NAMESPACES)

    if id_elements:
      id_element = id_elements[0]
      return id_element.get(u"Id", None), id_element.get(u"ChangeKey", None)
    else:
      return None, None
