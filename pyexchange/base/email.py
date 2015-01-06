import json

class BaseExchangeEmailService(object):
    """
    The base service for emails
    """
    def __init__(self, service, folder_id):
        self.service = service
        self.folder_id = folder_id

    def get_email(self, email_id):
        raise NotImplementedError

    def list_emails(self):
        raise NotImplementedError


class DataSerializerMixin(object):
    """
    The Mixing Expects a DATA_ATTRIBUTES attribute for fields that are serializable
    """

    def as_dict(self):
        """
        Serializes as a dictionary
        """
        result = {}
        for attr in self.DATA_ATTRIBUTES:
            val = getattr(self, attr, None)
            if val and hasattr(val, "as_dict"):
                as_dict_fn = getattr(val, "as_dict")
                result[attr] = as_dict_fn()
            elif isinstance(val, list):
                tmp_lst = []
                for v in val:
                    as_dict_fn = getattr(v, "as_dict", None)
                    if as_dict_fn:
                        d_tmp = as_dict_fn()
                        tmp_lst.append(d_tmp)
                    else:
                        tmp_lst.append(v)

                result[attr] = tmp_lst
            else:
                result[attr] = val

        return result

    def as_json(self):
        """
        Serializes as a JSON object
        """
        return json.dumps(self.as_dict())


class UpdatePropsMixin(object):

    def update_properties(self, properties):
        """
        From Dict to self attributes
        """
        for key in properties:
                if hasattr(self, key):
                    setattr(self, key, properties[key])



class ExchangeMailBoxItem(DataSerializerMixin, UpdatePropsMixin):
    """
    The mailbox item is responsible for describing the
    Users in the Exchange System like sender, recipients and etc
    """
    name = u''
    email = u''
    routing_type = u''

    DATA_ATTRIBUTES = [
        "name", "email"
    ]

    def __init__(self, **kw):
        self.update_properties(kw)


class BaseExchangeEmailItem(DataSerializerMixin, UpdatePropsMixin):
    """
    The base of the message itself
    """
    _id = None #It is the exchange identifier
    _change_key = None

    service = None
    folder_id = None

    #Fields in the email object
    subject = u''
    body_html = u''
    size = 0
    sent_time = None
    created_time = None
    received_time = None
    has_attachments = False
    is_read = False
    #These will keep only the ids do not keep the whole attachment
    #To get the attachment you need a second api call !!!
    attachments = None

    #Those need some special handling and are exposed via public fn
    _sender = None
    _recipients = []
    _cc_recipients = []

    DATA_ATTRIBUTES = [
        "subject", "body_html", "size", "sent_time", "received_time",
        "created_time", "has_attachments","is_read", "sender", "recipients",
        "cc_recipients", "attachments"]


    def __init__(self, service, id=None, folder_id=u'inbox', xml=None, **kwargs):
        """
        :param service:
        :param id:
        :param folder_id:
        :param xml:
        :param kwargs:
        :return:
        """
        self.service = service
        self.folder_id = folder_id

        if xml is not None:
            self._init_from_xml(xml)
        elif id is None:
            self.update_properties(kwargs)
        else:
            self._init_from_service(id)


    def _init_from_service(self, id):
        """ Connect to the Exchange service and grab all the properties out of it. """
        raise NotImplementedError


    def _init_from_xml(self, xml):
        """ Using already retrieved XML from Exchange, extract properties out of it. """
        raise NotImplementedError

    @property
    def sender(self):
        return self._sender

    @sender.setter
    def sender(self, sender_dict):
        self._sender = ExchangeMailBoxItem(**sender_dict)

    @property
    def recipients(self):
        return self._recipients

    @recipients.setter
    def recipients(self, recipient_lst):
        """
        Converts to the corresponding objects
        """
        self._recipients = self._recipient_list_conv(recipient_lst)

    @property
    def cc_recipients(self):
        return self._cc_recipients

    @cc_recipients.setter
    def cc_recipients(self, recipient_lst):
        """
        Converts to the corresponding objects
        """
        self._cc_recipients = self._recipient_list_conv(recipient_lst)


    def _recipient_list_conv(self, recipient_lst):
        result = []
        for r in recipient_lst:
            t = ExchangeMailBoxItem(**r)
            result.append(r)

        return result

    @property
    def id(self):
        """ **Read-only.** The internal id Exchange uses to refer to this event. """
        return self._id



class BaseExchangeAttachmentItem(DataSerializerMixin, UpdatePropsMixin):
    """
    The base of the message itself
    """
    _id = None #It is the exchange identifier

    service = None

    #Fields in the attachment object
    name = u''
    content_type = u''
    content_id = u''
    content = None

    DATA_ATTRIBUTES = [
        "id", "name", "content_type", "content_id"
    ]

    def __init__(self, service, id=None, xml=None, **kwargs):
        """
        :param service:
        :param id:
        :param folder_id:
        :param xml:
        :param kwargs:
        :return:
        """
        self.service = service

        if xml is not None:
            self._init_from_xml(xml)
        elif id is None:
            self.update_properties(kwargs)
        else:
            self._init_from_service(id)


    def _init_from_service(self, id):
        """ Connect to the Exchange service and grab all the properties out of it. """
        raise NotImplementedError


    def _init_from_xml(self, xml):
        """ Using already retrieved XML from Exchange, extract properties out of it. """
        raise NotImplementedError


    @property
    def id(self):
        """ **Read-only.** The internal id Exchange uses to refer to this event. """
        return self._id
