for env in $(find ${_YCDBIN} -maxdepth 1 -iname \*_venv) ; do
	vers=$(grep version ${env}/pyvenv.cfg | cut -d "=" -f 2 | tr -d ' ')
	if [ ! -d /usr/product/python/python-$vers ]
	then
		vers="${vers%.*}"
	fi
	/usr/product/python/python-$vers/bin/python ${_YCDBIN}/pairing_venv.py ${env}/ ;
done