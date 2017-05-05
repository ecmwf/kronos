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
static const char* temporary_filename3 = "_kronos_tests_tmp3";

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
    const char* print_string =  "[-1.234500e+02,[\"hello there\",false],true,null]";
    char buffer[53];
    char buffer2[100];
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

    /* Test writing to string */

    assert(write_json_string(buffer2, sizeof(buffer2), json) == strlen(print_string) + 1); /* Include '\0' in length */
    test_val = strncmp(print_string, buffer2, sizeof(buffer2));
    assert(test_val == 0);

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
    const char* src_string = "{\n\"a string\":\"hello there\", \"an-integer\": 999, \"array\":[1,2,3.14]}";
    const char* print_string = "{\"array\":[1,2,3.140000e+00],\"an-integer\":999,\"a string\":\"hello there\"}";
    char buffer[76];
    char buffer2[100];
    int test_val; /* Used to avoid c89 warnings with GCC */

    fp = create_open_tmp_file(src_string);
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

    /* Test writing to string */

    assert(write_json_string(buffer2, sizeof(buffer2), json) == strlen(print_string) + 1); /* Include '\0' in length */
    test_val = strncmp(print_string, buffer2, sizeof(buffer2));
    assert(test_val == 0);

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
    char buffer2[100];
    int test_val; /* Used to avoid c89 warnings with GCC */

    const char* correct_string = "JSON({\"array\":[1,2,3.140000e+00],\"an-integer\":999,\"a string\":\"hello there\"})";
    const char* src_string = "{\n\"a string\":\"hello there\", \"an-integer\": 999, \"array\":[1,2,3.14]}";
    const char* print_string = "{\"array\":[1,2,3.140000e+00],\"an-integer\":999,\"a string\":\"hello there\"}";

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

    /* Test writing to string */

    assert(write_json_string(buffer2, sizeof(buffer2), json) == strlen(print_string) + 1); /* Include '\0' in length */
    test_val = strncmp(print_string, buffer2, sizeof(buffer2));
    assert(test_val == 0);

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


void test_array_from_array() {

    double arr[4];
    JSON* json;

    int i;
    double d;

    arr[0] = 123.;
    arr[1] = 666.;
    arr[2] = 999.765;
    arr[3] = 1.;

    json = json_array_from_array(arr, 3);

    assert(json != 0);
    assert(json_is_array(json));
    assert(json_array_length(json) == 3);

    assert(json_as_integer(json_array_element(json, 0), &i) == 0);
    assert(i == 123);
    assert(json_as_integer(json_array_element(json, 1), &i) == 0);
    assert(i == 666);
    assert(json_as_double(json_array_element(json, 2), &d) == 0);
    assert(fabs(d - 999.765) < 1.0e-10);;

    free_json(json);

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

    /* Check that we can correctly extract a boolean element */

    assert(json_object_get_boolean(json, "elem1", &value) == 0);
    assert(value == true);
    assert(json_object_get_boolean(json, "elem3", &value) == 0);
    assert(value == false);

    /* Check that we correctly fail to get a non-boolean value */

    assert(json_object_get_boolean(json, "elem2", &value) != 0);

    /* Check that we correctly fail to get a non-existent value */

    assert(json_object_get_boolean(json, "invalid", &value) != 0);

    free_json(json);

    /* Check that we cannot extract a key from a non-object */

    json = json_from_string("1234");
    assert(json_object_get_boolean(json, "elem2", &value) != 0);
    free_json(json);
}


void test_object_get_double() {

    double value;

    JSON* json = json_from_string("{ \"elem1\": 123.45, \"elem2\": \"hi there\", \"elem3\": 666 }");

    /* Check that we can correctly extract a double element */

    assert(json_object_get_double(json, "elem1", &value) == 0);
    assert(fabs(value - 123.45) < 1.0e-10);
    assert(json_object_get_double(json, "elem3", &value) == 0);
    assert(fabs(value - 666.) < 1.0e-10);

    /* Check that we correctly fail to get a non-double value */

    assert(json_object_get_double(json, "elem2", &value) != 0);

    /* Check that we correctly fail to get a non-existent value */

    assert(json_object_get_double(json, "invalid", &value) != 0);

    free_json(json);

    /* Check that we cannot extract a key from a non-object */

    json = json_from_string("1234");
    assert(json_object_get_double(json, "elem2", &value) != 0);
    free_json(json);
}

void test_object_iterate() {

    double value;

    JSON* json = json_from_string("{ \"elem1\": 123.45, \"elem2\": \"hi there\", \"elem3\": 666 }");

    JSON* elem;
    int test_val;

    int i;
    double d;
    const char* c;

    elem = json_object_first(json);

    test_val = strcmp("elem3", elem->name);
    assert(test_val == 0);
    assert(json_as_integer(elem, &i) == 0);
    assert(i == 666);

    assert(elem->next != 0);
    elem = elem->next;

    test_val = strcmp("elem2", elem->name);
    assert(test_val == 0);
    assert(json_as_string_ptr(elem, &c) == 0);
    test_val = strcmp("hi there", c);
    assert(test_val == 0);

    assert(elem->next != 0);
    elem = elem->next;

    test_val = strcmp("elem1", elem->name);
    assert(test_val == 0);
    assert(json_as_double(elem, &d) == 0);
    assert(fabs(d - 123.45) < 1.0e-10);

    assert(elem->next == 0);

    free_json(json);
}


void test_allocate_json() {

    const char* str = "This is a test";
    char buffer[100];
    long x;
    double d;

    int test_val; /* Used to avoid c89 warnings with gcc */

    const JSON* cj;
    JSON* j2;
    JSON* j = json_null_new();

    assert(json_is_null(j));
    free_json(j);

    /* Test string construction */

    j = json_string_new(str);

    assert(json_is_string(j));
    assert(json_as_string(j, buffer, sizeof(buffer)) == 0);
    test_val = strncmp(buffer, str, 100);
    assert(test_val == 0);
    free_json(j);

    j = json_string_new_len(str, 3);
    assert(json_is_string(j));
    assert(json_as_string(j, buffer, sizeof(buffer)) == 0);
    test_val = strncmp(buffer, "Thi", 100);
    assert(test_val == 0);
    free_json(j);

    /* Test number-related jsons */

    j = json_number_new((int)666);
    assert(json_is_number(j));
    assert(json_as_integer(j, &x) == 0);
    assert(json_as_double(j, &d) == 0);
    assert(x == 666);
    assert(d == 666);
    free_json(j);

    j = json_number_new(123.456);
    assert(json_is_number(j));
    assert(json_as_double(j, &d) == 0);
    assert(fabs(d - 123.456) < 1.0e-6);
    free_json(j);

    /* Test the array construction */

    j = json_array_new();
    assert(json_is_array(j));
    assert(json_array_length(j) == 0);

    json_array_append(j, json_string_new("I am a test"));
    assert(json_is_array(j));
    assert(json_array_length(j) == 1);

    json_array_append(j, json_number_new(999));
    assert(json_is_array(j));
    assert(json_array_length(j) == 2);

    cj = json_array_element(j, 0);
    assert(json_is_string(cj));
    assert(json_string_length(cj) == 11);
    assert(json_as_string(cj, buffer, sizeof(buffer)) == 0);
    test_val = strncmp(buffer, "I am a test", sizeof(buffer));
    assert(test_val == 0);

    assert(json_as_integer(json_array_element(j, 1), &x) == 0);
    assert(x == 999);
    free_json(j);

    /* Test object construction */

    j = json_object_new();
    assert(json_is_object(j));
    assert(json_object_count(j) == 0);
    assert(!json_object_has(j, "key1"));
    assert(!json_object_has(j, "key2"));

    json_object_insert(j, "key1", json_string_new("I am another test"));
    assert(json_is_object(j));
    assert(json_object_count(j) == 1);
    assert(json_object_has(j, "key1"));
    assert(!json_object_has(j, "key2"));

    j2 = json_array_new();
    json_array_append(j2, json_number_new(4321));
    json_object_insert(j, "key2", j2);
    j2 = 0;
    assert(json_is_object(j));
    assert(json_object_count(j) == 2);
    assert(json_object_has(j, "key1"));
    assert(json_object_has(j, "key2"));

    cj = json_object_get(j, "key1");
    assert(json_is_string(cj));
    assert(json_as_string(cj, buffer, sizeof(buffer)) == 0);
    test_val = strncmp(buffer, "I am another test", sizeof(buffer));
    assert(test_val == 0);

    cj = json_object_get(j, "key2");
    assert(json_is_array(cj));
    assert(json_is_number(json_array_element(cj, 0)));
    assert(json_as_integer(json_array_element(cj, 0), &x) == 0);
    assert(x == 4321);
    free_json(j);
}


void test_print_string() {

    /* When printing, we need to ensure that things are correctly escaped.
     * We have already tested the escaping above, so check that it works correctly
     * at print time! */

    const char* src = "\"String \\\" with \\\\ all \\/ the \\b special \\f chars \\n ... \\r -> \\t\"";
    FILE* fp;
    JSON* json;
    int size;
    int test_val; /* Used to avoid c89 warnings with gcc */
    char buffer[256];

    json = json_from_string(src);

    fp = fopen(temporary_filename3, "w");
    write_json(fp, json);
    fclose(fp);

    fp = fopen(temporary_filename3, "r");
    size = fread(buffer, 1, sizeof(buffer), fp);
    fclose(fp);
    remove(temporary_filename3);

    assert(size == strlen(src));
    test_val = strncmp(src, buffer, size);
    assert(test_val == 0);

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
    test_array_from_array();
    test_invalid_parses();
    test_null_json();
    test_object_get_integer();
    test_object_get_boolean();
    test_object_get_double();
    test_object_iterate();
    test_allocate_json();
    test_print_string();
    return 0;
}
