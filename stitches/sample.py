from pkg_resources import resource_filename

import matplotlib.pyplot as plt
import matplotlib.image as img


def plot_my_dog():
    """Plot my dog Ava."""

    # get stock image of Ava
    dog = resource_filename('stitches', 'data/ava.png')

    # read in PNG
    im = img.imread(dog)

    return plt.imshow(im)
