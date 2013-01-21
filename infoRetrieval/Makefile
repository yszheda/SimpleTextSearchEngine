JAVAC = javac
CLASSPATH = /opt/hadoop/hadoop-core-1.0.3.jar
CLASSDIR = ./class
SRCDIR = ./src
TARGET = InfoRetrieval.jar

.SUFFIXES: .java .class

default: jar

classes:
#	make clean;
	mkdir -p $(CLASSDIR);
	$(JAVAC) -classpath $(CLASSPATH) -d $(CLASSDIR) $(SRCDIR)/*.java

jar:
	make classes;
	jar -cvf $(TARGET) -C $(CLASSDIR) .

clean:
	$(RM) *.jar;
	$(RM) -rf $(CLASSDIR)
