""" Sources:
http://www.fileformat.info/format/tiff/egff.htm
http://www.awaresystems.be/imaging/tiff/faq.html#q13
http://www.remotesensing.org/geotiff/spec/geotiff6.html
https://github.com/blink1073/tifffile
"""
import zlib
import numpy as np
from struct import unpack
from http.client import HTTPConnection


class Reader(object):
    """ Amazon Web Service Landsat Tiff Reader """

    def __init__(self, scene_id, band):

        host = 'landsat-pds.s3.amazonaws.com'
        self.url = '/L8/{0}/{1}/{2}/{2}_B{3}.TIF'.format(scene_id[3:6], scene_id[6:9], scene_id, band)
        self.conn = HTTPConnection(host)
        self.get_tiff_header()
        self.get_tags()
        self.geo_ascii_params = ''.join([i[0].decode('UTF-8') for i in self.geo_ascii_params])

    def get_range(self, start, end):
        self.conn.request('GET', self.url, headers={'Range': 'bytes={0}-{1}'.format(start, end)})
        resp = self.conn.getresponse()
        return resp.read()

    def get_tiff_header(self):
        header_bytes = self.get_range(0, 8)
        if header_bytes[2] != 42:
            raise Exception('This is not a valid Tiff format')
        self.byte_order = '>' if header_bytes[0] == 49 else None
        self.ifd_offset = unpack('i', header_bytes[4:8])[0]

    def read_tag_data(self, offset, count, dtype):

        data = self.get_range(offset, offset + count * DTYPE_SIZE[dtype])
        r = []
        for i in range(0, count):
            start = i * DTYPE_SIZE[dtype]
            end = i * DTYPE_SIZE[dtype] + DTYPE_SIZE[dtype]
            r.append(unpack(TIFF_DATA_TYPES[dtype], data[start:end]))
        return r

    def get_tags(self):
        ifd_tag_count_bytes = self.get_range(self.ifd_offset, self.ifd_offset + 1)
        ifd_tag_count = unpack('H', ifd_tag_count_bytes)[0]

        ifd_tag_bytes = self.get_range(self.ifd_offset + 2, self.ifd_offset + 2 + (ifd_tag_count * 12) + 4)

        self.tags = []

        for i in range(0, ifd_tag_count):
            tag = {}
            base = i * 12
            tag['id'] = unpack('H', ifd_tag_bytes[base:base + 2])[0]
            tag['name'] = TIFF_TAGS[tag['id']][0]
            tag['dtype'] = unpack('H', ifd_tag_bytes[base + 2:base + 4])[0]
            tag['count'] = unpack('i', ifd_tag_bytes[base + 4:base + 8])[0]
            tag['offset'] = unpack('i', ifd_tag_bytes[base + 8:base + 12])[0]

            if tag['count'] == 1:
                tag['data'] = tag['offset']
            else:
                tag['data'] = self.read_tag_data(tag['offset'], tag['count'], tag['dtype'])

            setattr(self, tag['name'], tag['data'])

            self.tags.append(tag)

    def get_number_of_tiles(self):

        tiles_across = (self.image_width + (self.tile_width - 1)) / self.tile_width
        tiles_down = (self.image_length + (self.tile_length - 1)) / self.tile_length
        self.tiles_in_image = tiles_across * tiles_down

    def get_tiles(self, tile_number):
        offset = self.tile_offsets[tile_number][0]
        count = self.tile_byte_counts[tile_number][0]
        tile_bytes = self.get_range(offset, offset + count)
        a = np.fromstring(zlib.decompress(tile_bytes),
                          dtype=TIFF_DATA_TYPES[3]).reshape((self.tile_width, self.tile_length))
        return a


# key is dtype, value is the number of bytes
DTYPE_SIZE = {
    1: 1,  # 1 bytes 8-bit unsigned integer
    2: 1,  # ASCII 8-bit, NULL-terminated string
    3: 2,  # SHORT 16-bit unsigned integer
    4: 4,  # LONG 32-bit unsigned integer
    5: 8,  # RATIONAL Two 32-bit unsigned integers
    6: 1,  # SBYTE 8-bit signed integer
    7: 1,  # UNDEFINE 8-bit byte
    8: 2,  # SSHORT 16-bit signed integer
    9: 4,  # SLONG 32-bit signed integer
    10: 8,  # SRATIONAL Two 32-bit signed integers
    11: 4,  # FLOAT 4-byte single-precision IEEE floating-point value
    12: 8,  # DOUBLE 8-byte double-precision IEEE floating-point value
}

TIFF_DATA_TYPES = {
    1: '1B',   # BYTE 8-bit unsigned integer.
    2: '1s',   # ASCII 8-bit byte that contains a 7-bit ASCII code;
               #   the last byte must be NULL (binary zero).
    3: '1H',   # SHORT 16-bit (2-byte) unsigned integer
    4: '1I',   # LONG 32-bit (4-byte) unsigned integer.
    5: '2I',   # RATIONAL Two LONGs: the first represents the numerator of
               #   a fraction; the second, the denominator.
    6: '1b',   # SBYTE An 8-bit signed (twos-complement) integer.
    7: '1s',   # UNDEFINED An 8-bit byte that may contain anything,
               #   depending on the definition of the field.
    8: '1h',   # SSHORT A 16-bit (2-byte) signed (twos-complement) integer.
    9: '1i',   # SLONG A 32-bit (4-byte) signed (twos-complement) integer.
    10: '2i',  # SRATIONAL Two SLONGs: the first represents the numerator
               #   of a fraction, the second the denominator.
    11: '1f',  # FLOAT Single precision (4-byte) IEEE format.
    12: '1d',  # DOUBLE Double precision (8-byte) IEEE format.
}


TIFF_TAGS = {
    254: ('new_subfile_type', 0, 4, 1, None),
    255: ('subfile_type', None, 3, 1,
          {0: 'undefined', 1: 'image', 2: 'reduced_image', 3: 'page'}),
    256: ('image_width', None, 4, 1, None),
    257: ('image_length', None, 4, 1, None),
    258: ('bits_per_sample', 1, 3, 1, None),
    259: ('compression', 1, 3, 1, None),
    262: ('photometric', None, 3, 1, None),
    266: ('fill_order', 1, 3, 1, {1: 'msb2lsb', 2: 'lsb2msb'}),
    269: ('document_name', None, 2, None, None),
    270: ('image_description', None, 2, None, None),
    271: ('make', None, 2, None, None),
    272: ('model', None, 2, None, None),
    273: ('strip_offsets', None, 4, None, None),
    274: ('orientation', 1, 3, 1, None),
    277: ('samples_per_pixel', 1, 3, 1, None),
    278: ('rows_per_strip', 2**32 - 1, 4, 1, None),
    279: ('strip_byte_counts', None, 4, None, None),
    280: ('min_sample_value', None, 3, None, None),
    281: ('max_sample_value', None, 3, None, None),  # 2**bits_per_sample
    282: ('x_resolution', None, 5, 1, None),
    283: ('y_resolution', None, 5, 1, None),
    284: ('planar_configuration', 1, 3, 1, {1: 'contig', 2: 'separate'}),
    285: ('page_name', None, 2, None, None),
    286: ('x_position', None, 5, 1, None),
    287: ('y_position', None, 5, 1, None),
    296: ('resolution_unit', 2, 4, 1, {1: 'none', 2: 'inch', 3: 'centimeter'}),
    297: ('page_number', None, 3, 2, None),
    305: ('software', None, 2, None, None),
    306: ('datetime', None, 2, None, None),
    315: ('artist', None, 2, None, None),
    316: ('host_computer', None, 2, None, None),
    317: ('predictor', 1, 3, 1, {1: None, 2: 'horizontal', 3: 'float'}),
    318: ('white_point', None, 5, 2, None),
    319: ('primary_chromaticities', None, 5, 6, None),
    320: ('color_map', None, 3, None, None),
    322: ('tile_width', None, 4, 1, None),
    323: ('tile_length', None, 4, 1, None),
    324: ('tile_offsets', None, 4, None, None),
    325: ('tile_byte_counts', None, 4, None, None),
    338: ('extra_samples', None, 3, None,
          {0: 'unspecified', 1: 'assocalpha', 2: 'unassalpha'}),
    339: ('sample_format', 1, 3, 1, None),
    340: ('smin_sample_value', None, None, None, None),
    341: ('smax_sample_value', None, None, None, None),
    347: ('jpeg_tables', None, 7, None, None),
    530: ('ycbcr_subsampling', 1, 3, 2, None),
    531: ('ycbcr_positioning', 1, 3, 1, None),
    32996: ('sgi_matteing', None, None, 1, None),  # use extra_samples
    32996: ('sgi_datatype', None, None, 1, None),  # use sample_format
    32997: ('image_depth', None, 4, 1, None),
    32998: ('tile_depth', None, 4, 1, None),
    33432: ('copyright', None, 1, None, None),
    33445: ('md_file_tag', None, 4, 1, None),
    33446: ('md_scale_pixel', None, 5, 1, None),
    33447: ('md_color_table', None, 3, None, None),
    33448: ('md_lab_name', None, 2, None, None),
    33449: ('md_sample_info', None, 2, None, None),
    33450: ('md_prep_date', None, 2, None, None),
    33451: ('md_prep_time', None, 2, None, None),
    33452: ('md_file_units', None, 2, None, None),
    33550: ('model_pixel_scale', None, 12, 3, None),
    33922: ('model_tie_point', None, 12, None, None),
    34665: ('exif_ifd', None, None, 1, None),
    34735: ('geo_key_directory', None, 3, None, None),
    34736: ('geo_double_params', None, 12, None, None),
    34737: ('geo_ascii_params', None, 2, None, None),
    34853: ('gps_ifd', None, None, 1, None),
    37510: ('user_comment', None, None, None, None),
    42112: ('gdal_metadata', None, 2, None, None),
    42113: ('gdal_nodata', None, 2, None, None),
    50289: ('mc_xy_position', None, 12, 2, None),
    50290: ('mc_z_position', None, 12, 1, None),
    50291: ('mc_xy_calibration', None, 12, 3, None),
    50292: ('mc_lens_lem_na_n', None, 12, 3, None),
    50293: ('mc_channel_name', None, 1, None, None),
    50294: ('mc_ex_wavelength', None, 12, 1, None),
    50295: ('mc_time_stamp', None, 12, 1, None),
    50838: ('imagej_byte_counts', None, None, None, None),
    51023: ('fibics_xml', None, 2, None, None),
    65200: ('flex_xml', None, 2, None, None),
    # code: (attribute name, default value, type, count, validator)
}

if __name__ == '__main__':
    r = Reader('LC80450342015359LGN00', 4)
    print(r.get_tiles(3))
