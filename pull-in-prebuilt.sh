# Where pre-build libyara is expected to come from.
mkdir -p prebuilt/include
mkdir -p prebuilt/pkgconfig
cp /usr/local/lib/libyara_x_capi.so* ./prebuilt/
cp -r /usr/local/include/* ./prebuilt/include/
cp -r /usr/local/lib/pkgconfig/* ./prebuilt/pkgconfig/