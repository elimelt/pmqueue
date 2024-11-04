mv docs docs_old

# build javadoc jar
mvn org.apache.maven.plugins:maven-javadoc-plugin:3.5.0:jar

if [ ! -d docs ]; then
    mkdir docs
fi

cp target/*-javadoc.jar docs/javadoc.jar


# go to docs directory
pushd docs

# unzip javadoc jar
mv javadoc.jar javadoc.jar.zip
unzip -o javadoc.jar.zip -d javadoc

# remove zip file
rm javadoc.jar.zip

# copy javadoc to parent directory
cp -r javadoc/* .

# remove javadoc directory
rm -rf javadoc

# go back to parent directory
popd

# remove old docs
rm -rf docs_old