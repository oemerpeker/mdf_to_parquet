Case study: https://www.csselectronics.com/pages/nissan-leaf-can-bus-obd2-soc-state-of-charge

This folder contains data from a Nissan Leaf 2019 (Channel 1), collected by using the included config to request UDS-style data.

On Channel 2, a CANmod.gps and CANmod.temp are connected, providing GNSS/IMU and temperature data.

Note that the Channel 1 data is multiframe (CAN ISO TP) - see also our intro to UDS. To decode this, you can use our Python API.