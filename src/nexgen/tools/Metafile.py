"""
Define a Metafile object to describe the _meta.h5 file and get the necessary information from it.
"""
from __future__ import annotations

import re
from functools import cached_property
from typing import Dict, List, Tuple

import h5py

__all__ = ["Metafile", "DectrisMetafile", "TristanMetafile"]

tristan_pattern = re.compile(r"ts_qty_module\d{2}")


class Metafile:
    def __init__(self, handle: h5py.File):
        self._handle = handle

    def __getitem__(self, key: str) -> h5py.Group | h5py.Dataset:
        return self._handle[key]

    def __len__(self):
        return len(self._handle)

    def __str__(self):
        return f"File {self._handle.filename} opened in '{self._handle.mode}' mode."

    @cached_property
    def walk(self) -> List[str]:
        obj_list = []
        self._handle.visit(obj_list.append)
        return obj_list

    @cached_property
    def hasMask(self):
        return "mask" in self.walk

    @cached_property
    def hasFlatfield(self):
        return "flatfield" in self.walk


class DectrisMetafile(Metafile):
    """
    Describes a _meta.h5 file for a Dectris Eiger detector.
    """

    def __init__(self, handle: h5py.File):
        super().__init__(handle)

    @cached_property
    def hasDectrisGroup(self) -> bool:
        return "_dectris" in self._handle.keys() and isinstance(
            self._handle["_dectris"], h5py.Group
        )

    @cached_property
    def hasConfig(self) -> bool:
        return "config" in self._handle.keys()

    def read_dectris_config(self) -> Dict:
        config = {}
        for k, v in self._handle["_dectris"].items():
            v = v[()]
            if len(v) == 1:
                v = v[0]
                if isinstance(v, bytes):
                    v = v.decode()
            config[k] = v
        return config

    def read_config_dset(self) -> Dict:
        return eval(self._handle["config"][()])

    def get_number_of_images(self) -> int:
        if self.hasDectrisGroup:
            config = self.read_dectris_config()
            if config["nimages"] >= 1 and config["ntrigger"] == 1:
                return config["nimages"]
            elif config["nimages"] == 1 and config["ntrigger"] > 1:
                # For example a "triggered" data collection
                return config["ntrigger"]
        else:
            _loc = [obj for obj in self.walk if "nimages" in obj]
            return self.__getitem__(_loc[0])[0]

    def get_detector_size(self) -> Tuple:
        # NB. returns (fast, slow) but data_size in nxs file shoud be recorded (slow, fast)
        # => det_size[::-1]
        _loc = [obj for obj in self.walk if "pixels_in_detector" in obj]
        det_size = [self.__getitem__(i)[0] for i in _loc]
        return None if not det_size else tuple(det_size[::-1])

    def get_pixel_size(self) -> List:
        _loc = [obj for obj in self.walk if "pixel_size" in obj]
        pix = [self.__getitem__(i)[0] for i in _loc]
        return None if not pix else pix

    def get_beam_center(self) -> List:
        _loc = [obj for obj in self.walk if "beam_center" in obj]
        bc = [self.__getitem__(i)[0] for i in _loc]
        return None if not bc else bc

    def get_wavelength(self) -> float:
        _loc = [obj for obj in self.walk if "wavelength" in obj]
        return None if not _loc else self.__getitem__(_loc[0])[0]

    def get_detector_distance(self) -> float:
        # Distance in Dectris meta file is in m.
        _loc = [obj for obj in self.walk if "detector_distance" in obj]
        return None if not _loc else self.__getitem__(_loc[0])[0]

    def get_saturation_value(self) -> float:
        _loc = [obj for obj in self.walk if "countrate_correction_count_cutoff" in obj]
        return None if not _loc else self.__getitem__(_loc[0])[0]

    def get_sensor_information(self) -> Tuple[bytes, float]:
        _loc_material = [obj for obj in self.walk if "sensor_material" in obj]
        _loc_thickness = [obj for obj in self.walk if "sensor_thickness" in obj]
        return (
            self.__getitem__(_loc_material[0])[0],
            self.__getitem__(_loc_thickness[0])[0],
        )

    def get_bit_depth_image(self):
        _loc = [obj for obj in self.walk if "bit_depth_image" in obj]
        return None if not _loc else self.__getitem__(_loc[0])[0]

    def find_mask(self) -> Tuple[str, str]:
        if self.hasMask:
            mask_path = [obj for obj in self.walk if obj.lower() == "mask"]
            if mask_applied_path := [
                obj for obj in self.walk if "mask_applied" in obj
            ]:
                return (mask_path[0], mask_applied_path[0])
            else:
                return (mask_path[0], None)
        return (None, None)

    def find_flatfield(self) -> Tuple[str, str]:
        if self.hasFlatfield:
            flatfield_path = [obj for obj in self.walk if obj.lower() == "flatfield"]
            if flatfield_applied_path := [
                obj for obj in self.walk if "flatfield_correction_applied" in obj
            ]:
                return (flatfield_path[0], flatfield_applied_path[0])
            else:
                return (flatfield_path[0], None)
        return (None, None)

    def find_software_version(self) -> str:
        _loc = [obj for obj in self.walk if "software_version" in obj]
        return None if not _loc else _loc[0]

    def find_threshold_energy(self) -> str:
        _loc = [obj for obj in self.walk if "threshold_energy" in obj]
        return None if not _loc else _loc[0]

    def find_bit_depth_readout(self) -> str:
        _loc = [obj for obj in self.walk if "bit_depth_readout" in obj]
        return None if not _loc else _loc[0]

    def find_bit_depth_image(self) -> str:
        _loc = [obj for obj in self.walk if "bit_depth_image" in obj]
        return None if not _loc else _loc[0]

    def find_detector_number(self) -> str:
        _loc = [obj for obj in self.walk if "detector_number" in obj]
        return None if not _loc else _loc[0]

    def find_detector_readout_time(self) -> str:
        _loc = [obj for obj in self.walk if "detector_readout_time" in obj]
        return None if not _loc else _loc[0]


class TristanMetafile(Metafile):
    """
    Describes a _meta.h5 file for a Tristan detector.
    """

    @staticmethod
    def isTristan(filename):
        with h5py.File(filename, "r") as fh:
            res = [k for k in fh.keys() if tristan_pattern.fullmatch(k)]
        return bool(res)

    def __init__(self, handle: h5py.File):
        super().__init__(handle)

    def find_number_of_modules(self) -> int:
        n_modules = [k for k in self._handle.keys() if tristan_pattern.fullmatch(k)]
        return len(n_modules)

    def find_software_version(self) -> str:
        _loc = [obj for obj in self.walk if "software_version" in obj]
        return None if not _loc else _loc[0]

    def find_meta_version(self) -> str:
        _loc = [obj for obj in self.walk if "meta_version" in obj]
        return None if not _loc else _loc[0]
