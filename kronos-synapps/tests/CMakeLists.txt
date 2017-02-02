
ecbuild_add_test(
    TARGET kronos_test_json
    SOURCES test_json.c
    LIBS kronos )

ecbuild_add_test(
    TARGET kronos_test_utility
    SOURCES test_utility.c
    LIBS kronos )

ecbuild_add_test(
    TARGET kronos_test_file_read
    SOURCES test_file_read.c
    LIBS kronos )

ecbuild_add_test(
    TARGET kronos_test_file_write
    SOURCES test_file_write.c
    LIBS kronos )

ecbuild_add_test(
    TARGET kronos_test_cpu
    SOURCES test_cpu.c
    LIBS kronos )

ecbuild_add_test(
    TARGET kronos_test_factories
    SOURCES test_factories.c
    LIBS kronos )


ecbuild_add_test(
    TARGET kronos_test_mpi
    SOURCES test_mpi.c
    CONDITION HAVE_MPI
    LIBS kronos )
