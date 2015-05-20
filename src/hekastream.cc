#include "message.pb.h"
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>

#include <R.h>
#include <Rinternals.h>
#include <Rdefines.h>
#include <Rmath.h>
#include <R_ext/Rdynload.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

using namespace google::protobuf;
using namespace google::protobuf::io;

// Download proto file from
// https://github.com/mozilla-services/heka/blob/dev/message/message.proto
// stream framing : https://hekad.readthedocs.org/en/latest/message/index.html#stream-framing

extern uintptr_t R_CStackLimit;

void CaptureLogInLibrary(LogLevel level, const char* filename, int line,
		const string& message) {
  static const char* pb_log_level[] = { "LOGLEVEL_INFO", "LOGLEVEL_WARNING",
			"LOGLEVEL_ERROR", "LOGLEVEL_FATAL", "LOGLEVEL_DFATAL" };
	Rf_error("hekastream PB ERROR[%s](%s:%d) %s", pb_log_level[level], filename, line,
			message.c_str());
}

#define DATASIZE 1024*1024
#define GROWDATA 1.5


extern "C" {
  // great docs
  // http://stackoverflow.com/questions/7032617/storing-c-objects-in-r
  struct hekacontrol {
    int fd;
    void *data;
    unsigned int size;
    int ngrows;
    unsigned int nread;
    unsigned int mbytes;
  };

  void R_init_hekastream(DllInfo *info) {
    R_CStackLimit = (uintptr_t) -1;
    google::protobuf::SetLogHandler(&CaptureLogInLibrary);
  }

  void clearstream(SEXP ext)
  {
    if (NULL == R_ExternalPtrAddr(ext))
      return;
    hekacontrol *c = (hekacontrol *) R_ExternalPtrAddr(ext);
    printf("Getting killed\n");
    if(c){
      if(c->data)  free(c->data);
      close(c->fd);
      free(c);
    }
    R_ClearExternalPtr(ext);
  }


  
  SEXP initializeWithFile(SEXP f,SEXP info){
    const char * filename = (const char*)CHAR(STRING_ELT(f,0));
    int fd = open(filename, O_RDONLY);
    if(fd<0){
      Rf_error(strerror(errno));
      return(R_NilValue);
    }
    struct hekacontrol * c = (struct hekacontrol *)malloc(sizeof(hekacontrol));
    c->fd = fd;
    c->data = (void *) malloc( DATASIZE );
    c->size = DATASIZE;
    c->mbytes=c->nread = c->ngrows = 0;
    SEXP ext = PROTECT(R_MakeExternalPtr(c, R_NilValue, info));
    R_RegisterCFinalizerEx(ext, clearstream, TRUE);
    UNPROTECT(1);
    return ext;
  }
  SEXP makeMessageJSON(message::Message m){
    return R_NilValue;
  }
  SEXP makeMessageR(message::Message m){
    SEXP m1;
    PROTECT(m1 = Rf_allocSExp(ENVSXP));
    if( m.has_uuid())
      Rf_defineVar(Rf_install("uuid"),ScalarString(mkChar(m.uuid().c_str())) ,m1);
    if( m.has_timestamp())
      Rf_defineVar(Rf_install("timestamp"),ScalarReal(m.timestamp()) ,m1);
    if( m.has_type())
      Rf_defineVar(Rf_install("type"),ScalarString(mkChar(m.type().c_str())) ,m1);
    if( m.has_logger())
      Rf_defineVar(Rf_install("logger"),ScalarString(mkChar(m.logger().c_str())) ,m1);
    if( m.has_severity())
      Rf_defineVar(Rf_install("severity"),ScalarInteger(m.severity()) ,m1);
    if( m.has_payload())
      Rf_defineVar(Rf_install("payload"),ScalarString(mkChar(m.payload().c_str())) ,m1);
    if( m.has_env_version())
      Rf_defineVar(Rf_install("env_version"),ScalarString(mkChar(m.env_version().c_str())) ,m1);
    if( m.has_pid())
      Rf_defineVar(Rf_install("pid"),ScalarInteger(m.pid()) ,m1);
    if( m.has_hostname())
      Rf_defineVar(Rf_install("hostname"),ScalarString(mkChar(m.hostname().c_str())) ,m1);
    int ml = m.fields_size();
    if(ml>0){
      SEXP f = PROTECT(Rf_allocVector(VECSXP, ml));
      SEXP fnames = PROTECT(Rf_allocVector(STRSXP, ml));
      for(int j=0; j<ml;j++){
        SEXP m2;
        PROTECT(m2 = Rf_allocSExp(ENVSXP));
        message::Field l = m.fields(j);
        if( l.has_name()){
          // Rf_defineVar(Rf_install("name"),ScalarString(mkChar(l.name().c_str())) ,m2);
          SET_STRING_ELT(fnames, j, mkChar(l.name().c_str()));
        }
        if( l.has_representation())
          Rf_defineVar(Rf_install("representation"),ScalarString(mkChar(l.representation().c_str())) ,m2);
        if(true){
          SEXP v;
          switch(l.value_type()){
          case message::Field_ValueType_STRING:
            {
              PROTECT(v = Rf_allocVector(STRSXP, l.value_string_size()));
              for(int j = 0; j<l.value_string_size();j++) {
                SET_STRING_ELT(v,j, mkChar(l.value_string(j).c_str()));
              }
              break;
            }
          case message::Field_ValueType_BYTES:
            {
              // only reading the first one
              if(l.value_bytes_size()>1)
                Rf_warning("Value is of type value_bytes, reading first though it is of length:%d", l.value_bytes_size());
              std::string ax = l.value_bytes(0);
              PROTECT(v = Rf_allocVector(RAWSXP, ax.size()));
              memcpy(RAW(v), ax.data(), ax.size());
              break;
            }
          case message::Field_ValueType_INTEGER:
            {
              PROTECT(v = Rf_allocVector(INTSXP, l.value_integer_size()));
              for(int j = 0; j<l.value_integer_size();j++) INTEGER(v)[j] = l.value_integer(j);
              break;
            }
          case message::Field_ValueType_DOUBLE:
            {
              PROTECT(v = Rf_allocVector(REALSXP, l.value_double_size()));
              for(int j = 0; j<l.value_double_size();j++) REAL(v)[j] = l.value_double(j);
              break;
            }
          case message::Field_ValueType_BOOL:
            {
              PROTECT(v = Rf_allocVector(LGLSXP, l.value_bool_size()));
              for(int j = 0; j<l.value_bool_size();j++) LOGICAL(v)[j] = l.value_bool(j);
              break;
            }
          }
          Rf_defineVar(Rf_install("value"),v ,m2);
          UNPROTECT(1); //value
        }
        SET_VECTOR_ELT(f, j, m2);
        UNPROTECT(1); // m2
      }
      Rf_setAttrib(f,Rf_install("names"), fnames);
      Rf_defineVar(Rf_install("fields"),f ,m1);
      UNPROTECT(2); // f and fnames
    }
    UNPROTECT(1); // m1
    return(m1);
  }

  SEXP getGrowth(SEXP ext){
    struct hekacontrol *c = (struct hekacontrol *)R_ExternalPtrAddr(ext);
    return ScalarInteger(c->ngrows);
  }
  SEXP getNRead(SEXP ext){
    struct hekacontrol *c = (struct hekacontrol *)R_ExternalPtrAddr(ext);
    return ScalarReal((double) (c->nread));
  }
  SEXP getMbytes(SEXP ext){
    struct hekacontrol *c = (struct hekacontrol *)R_ExternalPtrAddr(ext);
    return ScalarReal((double) (c->mbytes));
  }
  
  SEXP getOneObjectFromFile(SEXP ext,SEXP jsn){
    struct hekacontrol *c = (struct hekacontrol *)R_ExternalPtrAddr(ext);
    char buf;
    int r;
    r = read(c->fd,&buf, 1);
    if(r==0) return(R_NilValue); //finished
    if(r<0 || buf!=0x1E){
      Rf_error("hekaparse: could not read record separator: r=%d,buf=%d",r,(int)buf);
      return(R_NilValue);
    }
    r = read(c->fd,&buf, 1);
    if(r<=0){
      Rf_error("hekaparse: Error reading header length");
      return(R_NilValue);
    }
    read(c->fd, c->data,(int)buf);
    message::Header hd = message::Header();
    hd.ParseFromArray(c->data, (int)(buf));
    r = read(c->fd,&buf, 1);
    if(r<0 || buf!=0x1F){
      Rf_error("hekaparse: could not skip unit separator: r=%d,buf=%d",r,(int)buf);
      return(R_NilValue);
    }
    if(!hd.has_message_length()){
      Rf_error("hekaparse: No Message Length found in header");
      return(R_NilValue);
    }
    int mlength = hd.message_length();
    if( (c->size < mlength)){
      if(!realloc(c->data, mlength*GROWDATA)){
        Rf_error("hekaparse: Urgent, could not realloc memory");
        return(R_NilValue);
      }
      c->ngrows = c->ngrows+1;
      c->size = mlength *GROWDATA;
    }
    if(read(c->fd, c->data, mlength)<=0){
      Rf_error("hekaparse: Urgent, could not read message");
      return(R_NilValue);
    }  
    message::Message m = message::Message();
    m.ParseFromArray(c->data, mlength);
    c->nread +=1; c->mbytes += mlength;
    SEXP m1;
    if(LOGICAL(jsn)[0]) m1=makeMessageJSON(m); else  m1=makeMessageR(m);
    return(m1);
  }


  SEXP parseFramesFromArray(SEXP r,SEXP jsn){
      // One shot, instead of streaming.
      SEXP m1;
      char buffer[50]; //10^50 read ... i hope not!
      int usejson = LOGICAL(jsn)[0];
      PROTECT(m1 = Rf_allocSExp(ENVSXP));
      uint8 *data = RAW(r);
      unsigned int l = LENGTH(r), totalread = 0;
      unsigned int nread=0, mbytes = 0;
      while(totalread<l){
        if( data[totalread++] != 0x1E){
          Rf_error("hekaparse: invalid record separator: pos=%d,value=%d",totalread-1,(uint8)( data[totalread-1]));
          return(R_NilValue);
        }
        int buf = (int) data[totalread++];
        message::Header hd = message::Header();
        hd.ParseFromArray(data+totalread, (buf));
        totalread+=buf;
        if(data[totalread++]!=0x1F){
          Rf_error("hekaparse: invalid unit separator: pos=%d,value=%d",totalread-1,(uint8)( data[totalread-1]));
          return(R_NilValue);
        }
        if(!hd.has_message_length()){
          Rf_error("hekaparse: No Message Length found in header");
          return(R_NilValue);
        }
        int mlength = hd.message_length();
        message::Message m = message::Message();
        m.ParseFromArray(data+totalread, mlength);
        nread +=1; mbytes += mlength; totalread+=mlength;
        SEXP m2;
        if(LOGICAL(jsn)[0]) m2=makeMessageJSON(m); else  m2=makeMessageR(m);
        sprintf(buffer, "%d", nread);
        Rf_defineVar(Rf_install(buffer),m2 ,m1);
      }
      Rf_setAttrib(m1, Rf_install("nread"),ScalarReal((double)nread));
      Rf_setAttrib(m1, Rf_install("bytes"),ScalarReal((double)mbytes));
      UNPROTECT(1);
      return(m1);
    }
  
  
}


/*
 R CMD SHLIB rjq.c hekastream.cc message.pb.cc -ljq `pkg-config --libs protobuf` `pkg-config --cflags protobuf`

  aws s3 cp s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry/20150511/telemetry/4/main/Firefox/nightly/40.0a1/20150511235922.677_ip-172-31-14-40 .
  w=readBin("20150511235922.677_ip-172-31-14-40", what='raw',n=as.numeric(file.info("20150511235922.677_ip-172-31-14-40")['size']))

*/
