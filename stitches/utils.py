
import intake


def open_esm_datastore(url):
    """Convenience wrapper for the open_esm_datastore method from the intake package.

    :param url:                         URL to JSON data
    :type url:                          str

    :return:                            TODO: fill in info

    """

    return intake.open_esm_datastore(url)
