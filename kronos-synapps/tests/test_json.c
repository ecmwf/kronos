/*
 * (C) Copyright 1996-2015 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/**
 * @date May 2016
 * @author Simon Smart
 */

/* We want to ensure that assert is always defined */
#ifdef NDEBUG
#undef NDEBUG
#include <assert.h>
#define NDEBUG
#else
#include <assert.h>
#endif

#include <stdio.h>
#include <string.h>
#include <math.h>
#include <stdlib.h>

#include "kronos/bool.h"
#include "kronos/json.h"

/* ------------------------------------------------------------------------------------------------------------------ */

/*
 * A couple of quick utility functions
 */

static const char* temporary_filename = "_kronos_tests_tmp";
static const char* temporary_filename2 = "_kronos_tests_tmp2";

FILE* create_open_tmp_file(const char* data) {

    FILE* fp = fopen(temporary_filename, "w");
    assert(fp);

    fwrite(data, 1, strlen(data), fp);
    fclose(fp);

    fp = fopen(temporary_filename, "r");
    assert(fp);
    return fp;
}


void fclose_tmp_file(FILE* fp) {
    assert(fp);
    fclose(fp);

    remove(temporary_filename);
}


/* ------------------------------------------------------------------------------------------------------------------ */


void test_parse_boolean_true() {
    
    FILE* fp;
    JSON* json;

    bool val;
    long int_val;
    double double_val;
    const char* str_val;

    fp = create_open_tmp_file("true");
    assert(fp);

    json = parse_json(fp);
    assert(json);

    assert(json->type == JSON_BOOLEAN);
    assert(json->boolean);

    assert(!json_is_null(json));
    assert(json_is_boolean(json));
    assert(!json_is_number(json));
    assert(!json_is_string(json));
    assert(!json_is_object(json));
    assert(!json_is_array(json));

    /* Test that the accessor functions work correctly */

    assert(json_as_boolean(json, &val) == 0);
    assert(val);

    assert(json_as_integer(json, &int_val) != 0);
    assert(json_as_double(json, &double_val) != 0);
    assert(json_as_string_ptr(json, &str_val) != 0);

    free_json(json);
    fclose_tmp_file(fp);
}

void test_parse_boolean_false() {
    
    FILE* fp;
    JSON* json;

    bool val;
    long int_val;
    double double_val;
    const char* str_val;

    fp = create_open_tmp_file("false");
    assert(fp);

    json = parse_json(fp);
    assert(json);

    assert(json->type == JSON_BOOLEAN);
    assert(!json->boolean);

    assert(!json_is_null(json));
    assert(json_is_boolean(json));
    assert(!json_is_number(json));
    assert(!json_is_string(json));
    assert(!json_is_object(json));
    assert(!json_is_array(json));

    /* Test that the accessor functions work correctly */

    assert(json_as_boolean(json, &val) == 0);
    assert(!val);

    assert(json_as_integer(json, &int_val) != 0);
    assert(json_as_double(json, &double_val) != 0);
    assert(json_as_string_ptr(json, &str_val) != 0);

    free_json(json);
    fclose_tmp_file(fp);
}


void test_parse_string() {

    FILE* fp;
    JSON* json;

    const char* str_ptr;
    char str_arr[100];
    long int_val;
    double double_val;
    bool bool_val;
    int test_val; /* Used to avoid c89 warnings with GCC */

    fp = create_open_tmp_file("\"String \\\" with \\\\ all \\/ the \\b special \\f chars \\n ... \\r -> \\t\"");
    assert(fp);

    json = parse_json(fp);
    assert(json);

    assert(json->type == JSON_STRING);
    assert(json->string != NULL);

    assert(!json_is_null(json));
    assert(!json_is_boolean(json));
    assert(!json_is_number(json));
    assert(json_is_string(json));
    assert(!json_is_object(json));
    assert(!json_is_array(json));

    /* Test that the accessor functions work correctly */

    assert(json_string_length(json) == 56);

    assert(json_as_string_ptr(json, &str_ptr) == 0);
    test_val = strncmp(str_ptr, "String \" with \\ all / the \b special \f chars \n ... \r -> \t", 57);
    assert(test_val == 0);

    assert(json_as_string(json, str_arr, sizeof(str_arr)) == 0);
    test_val = strncmp(str_ptr, "String \" with \\ all / the \b special \f chars \n ... \r -> \t", 57);
    assert(test_val == 0);

    assert(json_as_integer(json, &int_val) != 0);
    assert(json_as_double(json, &double_val) != 0);
    assert(json_as_boolean(json, &bool_val) != 0);

    free_json(json);
    fclose_tmp_file(fp);
}


void test_parse_string_empty() {

    FILE* fp;
    JSON* json;
    const char* str_ptr;
    char* mutable_str_ptr;
    int test_val; /* Used to avoid c89 warnings with GCC */

    fp = create_open_tmp_file("\"\"");
    assert(fp);

    json = parse_json(fp);
    assert(json);

    assert(json_is_string(json));
    assert(json_string_length(json) == 0);

    assert(json_as_string_ptr(json, &str_ptr) == 0);
    test_val = strncmp(str_ptr, "", 1);
    assert(test_val == 0);

    assert((mutable_str_ptr = json_string_strdup(json)) != NULL);
    assert(strlen(mutable_str_ptr) == 0);

    free(mutable_str_ptr);
    free_json(json);
    fclose_tmp_file(fp);
}


void test_parse_integer() {

    FILE* fp;
    JSON* json;

    long val;
    double double_val;
    bool bool_val;
    const char* str_val;

    fp = create_open_tmp_file("123456");
    assert(fp);

    json = parse_json(fp);
    assert(json);

    assert(json->type == JSON_NUMBER);
    assert((int)json->number == 123456);

    assert(!json_is_null(json));
    assert(!json_is_boolean(json));
    assert(json_is_number(json));
    assert(!json_is_string(json));
    assert(!json_is_object(json));
    assert(!json_is_array(json));

    /* Test that the accessor functions work correctly */

    assert(json_as_integer(json, &val) == 0);
    assert(val == 123456);

    assert(json_as_double(json, &double_val) == 0);
    assert(fabs(double_val - 123456) < 1.0e-12);

    assert(json_as_boolean(json, &bool_val) != 0);
    assert(json_as_string_ptr(json, &str_val) != 0);

    free_json(json);
    fclose_tmp_file(fp);
}


void test_parse_negative_integer() {

    FILE* fp;
    JSON* json;

    long val;
    double double_val;
    bool bool_val;
    const char* str_val;

    fp = create_open_tmp_file("-123456");
    assert(fp);

    json = parse_json(fp);
    assert(json);

    assert(json->type == JSON_NUMBER);
    assert((int)json->number == -123456);

    assert(!json_is_null(json));
    assert(!json_is_boolean(json));
    assert(json_is_number(json));
    assert(!json_is_string(json));
    assert(!json_is_object(json));
    assert(!json_is_array(json));

    /* Test that the accessor functions work correctly */

    assert(json_as_integer(json, &val) == 0);
    assert(val == -123456);

    assert(json_as_double(json, &double_val) == 0);
    assert(fabs(double_val + 123456) < 1.0e-12);

    assert(json_as_boolean(json, &bool_val) != 0);
    assert(json_as_string_ptr(json, &str_val) != 0);

    free_json(json);
    fclose_tmp_file(fp);
}


void test_parse_double() {

    FILE* fp;
    JSON* json;

    long val;
    double double_val;
    bool bool_val;
    const char* str_val;

    fp = create_open_tmp_file("1234.567");
    assert(fp);

    json = parse_json(fp);
    assert(json);

    assert(json->type == JSON_NUMBER);

    assert(!json_is_null(json));
    assert(!json_is_boolean(json));
    assert(json_is_number(json));
    assert(!json_is_string(json));
    assert(!json_is_object(json));
    assert(!json_is_array(json));

    /* Test that the accessor functions work correctly */

    assert(json_as_integer(json, &val) == 0);
    assert(val == 1234);

    assert(json_as_double(json, &double_val) == 0);
    assert(fabs(double_val - 1234.567) < 1.0e-12);

    assert(json_as_boolean(json, &bool_val) != 0);
    assert(json_as_string_ptr(json, &str_val) != 0);

    free_json(json);
    fclose_tmp_file(fp);
}


void test_parse_double2() {

    FILE* fp;
    JSON* json;

    double val;
    bool bool_val;
    const char* str_val;

    fp = create_open_tmp_file("123.45e67");
    assert(fp);

    json = parse_json(fp);
    assert(json);

    assert(json->type == JSON_NUMBER);

    assert(!json_is_null(json));
    assert(!json_is_boolean(json));
    assert(json_is_number(json));
    assert(!json_is_string(json));
    assert(!json_is_object(json));
    assert(!json_is_array(json));

    /* Test that the accessor functions work correctly */

    assert(json_as_double(json, &val) == 0);
    assert(fabs(val - 123.45e67) < 1.0);

    assert(json_as_boolean(json, &bool_val) != 0);
    assert(json_as_string_ptr(json, &str_val) != 0);

    free_json(json);
    fclose_tmp_file(fp);
}

void test_parse_negative_double() {

    FILE* fp;
    JSON* json;

    double val;
    bool bool_val;
    const char* str_val;

    fp = create_open_tmp_file("-123.45e-67");
    assert(fp);

    json = parse_json(fp);
    assert(json);

    assert(json->type == JSON_NUMBER);

    assert(!json_is_null(json));
    assert(!json_is_boolean(json));
    assert(json_is_number(json));
    assert(!json_is_string(json));
    assert(!json_is_object(json));
    assert(!json_is_array(json));

    /* Test that the accessor functions work correctly */

    assert(json_as_double(json, &val) == 0);
    assert(fabs(val + 123.45e-67) < 1e-72);

    assert(json_as_boolean(json, &bool_val) != 0);
    assert(json_as_string_ptr(json, &str_val) != 0);

    free_json(json);
    fclose_tmp_file(fp);
}


void test_parse_array() {

    FILE *fp, *fp2;
    JSON* json;

    const char* correct_string = "JSON([-1.234500e+02,[\"hello there\",false],true,null])";
    char buffer[53];
    const JSON* elem;
    int test_val; /* Used to avoid c89 warnings with GCC */

    fp = create_open_tmp_file("[-123.45, [\"hello there\", false], true, null]");
    assert(fp);

    json = parse_json(fp);
    assert(json);

    assert(json->type == JSON_ARRAY);
    assert(json->array);
    assert(json->count == 4);

    assert(!json_is_null(json));
    assert(!json_is_boolean(json));
    assert(!json_is_number(json));
    assert(!json_is_string(json));
    assert(!json_is_object(json));
    assert(json_is_array(json));

    assert(json_array_length(json) == 4);

    /* Test the printing routine */

    fp2 = fopen(temporary_filename2, "w");
    print_json(fp2, json);
    fclose(fp2);
    fp2 = fopen(temporary_filename2, "r");

    fread(buffer, 1, 53, fp2);
    test_val = strncmp(correct_string, buffer, strlen(correct_string));
    assert(test_val == 0);
    fclose(fp2);
    remove(temporary_filename2);

    /* Test that the elements are correct */

    elem = json_array_element(json, 0);
    assert(elem);
    assert(json_is_number(elem));

    elem = json_array_element(json, 2);
    assert(elem);
    assert(json_is_boolean(elem));

    elem = json_array_element(json, 3);
    assert(elem);
    assert(json_is_null(elem));

    elem = json_array_element(json, 1);
    assert(elem);
    assert(json_is_array(elem));
    assert(json_array_element(elem, 0));
    assert(json_is_string(json_array_element(elem, 0)));
    assert(json_array_element(elem, 0));
    assert(json_is_boolean(json_array_element(elem, 1)));

    free_json(json);
    fclose_tmp_file(fp);
}


void test_parse_object() {

    FILE *fp, *fp2;
    JSON* json;

    const char* correct_string = "JSON({\"array\":[1,2,3.140000e+00],\"an-integer\":999,\"a string\":\"hello there\"})";
    char buffer[76];
    int test_val; /* Used to avoid c89 warnings with GCC */

    fp = create_open_tmp_file("{\n\"a string\":\"hello there\", \"an-integer\": 999, \"array\":[1,2,3.14]}");
    assert(fp);

    json = parse_json(fp);
    assert(json);

    assert(!json_is_null(json));
    assert(!json_is_boolean(json));
    assert(!json_is_number(json));
    assert(!json_is_string(json));
    assert(json_is_object(json));
    assert(!json_is_array(json));

    /* Test the printing routine */

    fp2 = fopen(temporary_filename2, "w");
    print_json(fp2, json);
    fclose(fp2);
    fp2 = fopen(temporary_filename2, "r");

    fread(buffer, 1, 76, fp2);
    /*fprintf(stderr, "\n\ncorrect_string: %s|\n", correct_string);
    fprintf(stderr, "   read_string: %s|\n\n", buffer);
    fprintf(stderr, "    characters: %d|\n\n", strlen(correct_string));*/
    test_val = strncmp(correct_string, buffer, strlen(correct_string));
    assert(test_val == 0);
    fclose(fp2);
    remove(temporary_filename2);

    /* Test that the elements are correct */

    assert(json_object_count(json) == 3);

    assert(json_object_has(json, "a string"));
    assert(json_object_has(json, "an-integer"));
    assert(json_object_has(json, "array"));
    assert(!json_object_has(json, "invalid"));

    assert(json_is_string(json_object_get(json, "a string")));
    assert(json_is_number(json_object_get(json, "an-integer")));
    assert(json_is_array(json_object_get(json, "array")));
    assert(json_object_get(json, "invalid") == NULL);

    free_json(json);
    fclose_tmp_file(fp);
}

void test_parse_from_string() {

    FILE* fp2;
    JSON* json;
    char buffer[76];
    int test_val; /* Used to avoid c89 warnings with GCC */

    const char* correct_string = "JSON({\"array\":[1,2,3.140000e+00],\"an-integer\":999,\"a string\":\"hello there\"})";
    const char* src_string = "{\n\"a string\":\"hello there\", \"an-integer\": 999, \"array\":[1,2,3.14]}";

    json = json_from_string(src_string);
    assert(json);

    assert(!json_is_null(json));
    assert(!json_is_boolean(json));
    assert(!json_is_number(json));
    assert(!json_is_string(json));
    assert(json_is_object(json));
    assert(!json_is_array(json));

    /* Test the printing routine */

    fp2 = fopen(temporary_filename2, "w");
    print_json(fp2, json);
    fclose(fp2);
    fp2 = fopen(temporary_filename2, "r");

    fread(buffer, 1, 76, fp2);
    /*fprintf(stderr, "\n\ncorrect_string: %s|\n", correct_string);
    fprintf(stderr, "   read_string: %s|\n\n", buffer);
    fprintf(stderr, "    characters: %d|\n\n", strlen(correct_string));*/
    test_val = strncmp(correct_string, buffer, strlen(correct_string));
    assert(test_val == 0);
    fclose(fp2);
    remove(temporary_filename2);

    /* Test that the elements are correct */

    assert(json_object_count(json) == 3);

    assert(json_object_has(json, "a string"));
    assert(json_object_has(json, "an-integer"));
    assert(json_object_has(json, "array"));
    assert(!json_object_has(json, "invalid"));

    assert(json_is_string(json_object_get(json, "a string")));
    assert(json_is_number(json_object_get(json, "an-integer")));
    assert(json_is_array(json_object_get(json, "array")));
    assert(json_object_get(json, "invalid") == NULL);

    free_json(json);
}

void test_invalid_parses() {

    int i;
    FILE* fp;

    const char* invalid_parses[] = {
        "tr!!",
        "fal!!",
        "\"An unterminated string",
        "\"unicode \\u1234\"",
        "\"unexpected \\z\"",
        "-invalid number",
        "{\"unterminated\":\"object\",\"key\":1234",
        "{\"missing\":\"comma\"\"key\":1234}",
        "{\"missing\"\"colon\",\"key\":1234}",
        "{\"badstring:1234}",
        "{\"missing\":\"badstring}",
        "[ {\"key\",3 } ]"
    };

    for (i = 0; i < (sizeof(invalid_parses) / sizeof(invalid_parses[0])); i++) {

        fp = create_open_tmp_file(invalid_parses[i]);
        assert(fp);

        assert(parse_json(fp) == NULL);

        fclose_tmp_file(fp);
    }
}


void test_null_json() {

    /* Access a global null-json instance (i.e. one that does not need to be freed) */

    const JSON* null1 = null_json();
    const JSON* null2 = null_json();

    assert(json_is_null(null1));
    assert(json_is_null(null2));
    assert(null1 == null2);
}


void test_object_get_integer() {

    long value;
    JSON* json = json_from_string("{ \"elem1\": 1234, \"elem2\": \"hi there\" }");

    /* Check that we can correctly extract an integer element */

    assert(json_object_get_integer(json, "elem1", &value) == 0);
    assert(value == 1234);

    /* Check that we correctly fail to get a non-integer value */

    assert(json_object_get_integer(json, "elem2", &value) != 0);

    /* Check that we correctly fail to get a non-existent value */

    assert(json_object_get_integer(json, "invalid", &value) != 0);

    free_json(json);

    /* Check that we cannot extract a key from a non-object */

    json = json_from_string("1234");
    assert(json_object_get_integer(json, "elem2", &value) != 0);
    free_json(json);
}


void test_object_get_boolean() {

    bool value;
    JSON* json = json_from_string("{ \"elem1\": true, \"elem2\": \"hi there\", \"elem3\": false }");

    /* Check that we can correctly extract an integer element */

    assert(json_object_get_boolean(json, "elem1", &value) == 0);
    assert(value == true);
    assert(json_object_get_boolean(json, "elem3", &value) == 0);
    assert(value == false);

    /* Check that we correctly fail to get a non-integer value */

    assert(json_object_get_boolean(json, "elem2", &value) != 0);

    /* Check that we correctly fail to get a non-existent value */

    assert(json_object_get_boolean(json, "invalid", &value) != 0);

    free_json(json);

    /* Check that we cannot extract a key from a non-object */

    json = json_from_string("1234");
    assert(json_object_get_boolean(json, "elem2", &value) != 0);
    free_json(json);
}


/* ------------------------------------------------------------------------------------------------------------------ */

int main() {
    test_parse_boolean_true();
    test_parse_boolean_false();
    test_parse_string();
    test_parse_string_empty();
    test_parse_integer();
    test_parse_negative_integer();
    test_parse_double();
    test_parse_double2();
    test_parse_array();
    test_parse_object();
    test_parse_from_string();
    test_parse_negative_double();
    test_invalid_parses();
    test_null_json();
    test_object_get_integer();
    test_object_get_boolean();
    return 0;
}
