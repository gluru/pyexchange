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


class BaseExchangeEmailItem(object):
    """
    The base of the message itself
    """
    _id = None #It is the exchange identifier

    service = None
    folder_id = None

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
            self._update_properties(kwargs)
        else:
            self._init_from_service(id)


    def _init_from_service(self, id):
        """ Connect to the Exchange service and grab all the properties out of it. """
        raise NotImplementedError


    def _init_from_xml(self, xml):
        """ Using already retrieved XML from Exchange, extract properties out of it. """
        raise NotImplementedError

    def _update_properties(self, properties):
        for key in properties:
            setattr(self, key, properties[key])

    @property
    def id(self):
        """ **Read-only.** The internal id Exchange uses to refer to this event. """
        return self._id
