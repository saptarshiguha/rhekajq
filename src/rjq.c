
#include <R.h>
#include <Rinternals.h>
#include <Rdefines.h>
#include <Rmath.h>
#include <R_ext/Rdynload.h>


// #ifdef __cplusplus
// extern "C" {
// #endif
  
#include <jq.h>
#include <jv.h>


#define MAPVALUEDFT 120*1024*1024

struct jqcontrol {
  jq_state* jq;
  char *off;
  unsigned int size;
};

SEXP  mapvalueConcat(SEXP ext,SEXP s){
  if (NULL == R_ExternalPtrAddr(ext))
    return(R_NilValue);;
  struct jqcontrol *c = (struct jqcontrol *) R_ExternalPtrAddr(ext);
  int t=1+LENGTH(s)-1+1+1;
  int *ll= (int*) malloc(sizeof(int)*LENGTH(s));
  for(int i=0;i<LENGTH(s);i++){
    ll[i] = strlen(CHAR( STRING_ELT(VECTOR_ELT(s,i),0) ));
    t=t+ll[i];
  }
  if( t> c->size){
    c->off = (char *)realloc(c->off, t*1.5);
    if(!c->off) Rf_error("FATAL: could not make  map.value string");
    c->size = t*1.5;
  }
  char *off = c->off,*y=c->off;

  off[0] = '[';off++;
  int i;
  for(i=0;i<LENGTH(s)-1;i++){
    memcpy(off,CHAR( STRING_ELT(VECTOR_ELT(s,i),0)), ll[i]);
    off += ll[i];
    *off=',';
    off++;
  }
  memcpy(off,CHAR( STRING_ELT(VECTOR_ELT(s,i),0)), ll[i]);
  off += ll[i];
  *off=']'; off++; *off='\0';
  free(ll);

  return(R_NilValue);
}

  void my_err_cb(void *data, jv e) {
    e = jq_format_error(e);
    Rf_error( "rjq: %s", jv_string_value(e));
    jv_free(e);
  }
  
  void clearjq(SEXP ext)
  {
    if (NULL == R_ExternalPtrAddr(ext))
      return;
    struct jqcontrol *c = (struct jqcontrol *) R_ExternalPtrAddr(ext);
    if(c){
      jq_teardown(&(c->jq));
      free(c->off);
      free(c);
  }
    R_ClearExternalPtr(ext);
  }

  SEXP rjqinit(SEXP info){
    struct jqcontrol * c = (struct jqcontrol *)malloc(sizeof(struct jqcontrol));
    c->jq = jq_init();
    c->off = (char *) malloc(MAPVALUEDFT);
    if(!c->off) Rf_error("Could not allocate stub data for string handling\n");
    *(c->off)='\0';
    c->size = MAPVALUEDFT;
    jq_set_error_cb(c->jq,my_err_cb, NULL);
    SEXP ext = PROTECT(R_MakeExternalPtr(c, R_NilValue, info));
    R_RegisterCFinalizerEx(ext, clearjq, TRUE);
    UNPROTECT(1);
    return ext;
  }
  
  SEXP rjqprogargs(SEXP ext,SEXP fm)
  {
    struct jqcontrol *c = (struct jqcontrol *) R_ExternalPtrAddr(ext);
    return ScalarInteger(jq_compile_args(c->jq,CHAR(STRING_ELT(fm,0)),jv_array())); // 0 is error
  }

  
  SEXP rjqparse(SEXP ext,SEXP b){
    // must call jqprogargs before
    struct jqcontrol *c = (struct jqcontrol *) R_ExternalPtrAddr(ext);
    jv f;
    if(LENGTH(b)==0)
      f= jv_parse(c->off);
    else
      f= jv_parse(CHAR(STRING_ELT(b,0)));
    jq_start(c->jq,f,0);
    jv result,resultarray = jv_array();
    int ret=14;
    while (jv_is_valid(result = jq_next(c->jq))){
      if (jv_get_kind(result) == JV_KIND_FALSE || jv_get_kind(result) == JV_KIND_NULL)
        ret = 11;
      else
        ret = 0;
      resultarray = jv_array_append(resultarray,jv_copy(result));
    }
    
    if (jv_invalid_has_msg(jv_copy(result))) {
      jv msg = jv_invalid_get_msg(jv_copy(result));
      if (jv_get_kind(msg) == JV_KIND_STRING) {
        Rf_error("rjq: error: %s\n", jv_string_value(msg));
      } else {
        msg = jv_dump_string(msg, 0);
        Rf_error("rjq: error (but not a string): %s\n", jv_string_value(msg));
      }
      ret = 5;
      jv_free(msg);
    }
    
    resultarray = jv_dump_string(resultarray,0);
    SEXP r=R_NilValue;
    PROTECT(r=ScalarString(mkCharCE(jv_string_value(resultarray),CE_UTF8)));
    Rf_setAttrib(r, Rf_install("result"),ScalarInteger(ret));
    jv_free(resultarray);
    jv_free(result);
    UNPROTECT(1);
    return(r);
  }


    

  SEXP rjpretty(SEXP ext, SEXP b,SEXP clr,SEXP srt){
    // Will return a prettified ascii json which you can cat
    // if requested will colorify the output too term
    // and can optional sort the keys 
    struct jqcontrol *c = (struct jqcontrol *) R_ExternalPtrAddr(ext);
    jq_compile_args(c->jq,".",jv_array());
    jv f = jv_parse(CHAR(STRING_ELT(b,0)));
    jq_start(c->jq,f,0);
    jv result,lastgood;
    int ret=14;
    
    while (jv_is_valid(result = jq_next(c->jq))){
      if (jv_get_kind(result) == JV_KIND_FALSE || jv_get_kind(result) == JV_KIND_NULL)
        ret = 11;
      else
        ret = 0;
      lastgood = jv_copy(result);
    }
    
    if (jv_invalid_has_msg(jv_copy(result))) {
      jv msg = jv_invalid_get_msg(jv_copy(result));
      if (jv_get_kind(msg) == JV_KIND_STRING) {
        Rf_error("rjq: error: %s\n", jv_string_value(msg));
      } else {
        msg = jv_dump_string(msg, 0);
        Rf_error("rjq: error (but not a string): %s\n", jv_string_value(msg));
      }
      ret = 5;
      jv_free(msg);
    }
    
    jv jstr = jv_dump_string(lastgood, JV_PRINT_PRETTY|JV_PRINT_ASCII| (LOGICAL(srt)[0] ? JV_PRINT_SORTED :0) |  (LOGICAL(clr)[0]==1 ? JV_PRINT_COLOUR :0)  );
    SEXP r=R_NilValue;
    PROTECT(r=ScalarString(mkCharCE(jv_string_value(jstr),CE_UTF8)));
    Rf_setAttrib(r, Rf_install("result"),ScalarInteger(ret));
    UNPROTECT(1);
    jv_free(jstr);
    jv_free(result);
    return(r);
  }

// #ifdef __cplusplus
// }
// #endif
