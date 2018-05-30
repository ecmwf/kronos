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
 * @date May 2018
 * @author Simon Smart
 */

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "kronos/global_config.h"
#include "kronos/json.h"
#include "kronos/notification.h"
#include "kronos/trace.h"
#include "kronos/utility.h"

/* ------------------------------------------------------------------------------------------------------------------ */

int open_notification_connection() {

    const GlobalConfig* global_conf = global_config_instance();

    int result, sockfd;
    char portBuffer[128];
    struct addrinfo hints;
    struct addrinfo* servinfo;
    struct addrinfo* p;

    void* addr;
    char str_addr[INET6_ADDRSTRLEN];

    /* Get socket/connection info from getaddrinfo --> ipv4/6 compatible */

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    snprintf(portBuffer, sizeof(portBuffer), "%li", global_conf->notification_port);

    if ((result = getaddrinfo(global_conf->notification_host, portBuffer, &hints, &servinfo)) != 0) {
        fprintf(stderr, "getaddrinfo failed: (%d) %s\n", result, gai_strerror(result));
        return -1;
    }

    /* Loop through the results from getaddr info and connect to the first we can */

    for (p = servinfo; p != NULL; p = p->ai_next) {

        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            fprintf(stderr, "Failed to create socket: (%d) %s\n", errno, strerror(errno));
            continue;
        }

        if (p->ai_addr->sa_family == AF_INET) {
            addr = &((struct sockaddr_in*)p->ai_addr)->sin_addr;
        } else {
            addr = &((struct sockaddr_in6*)p->ai_addr)->sin6_addr;
        }
        inet_ntop(p->ai_family, addr, str_addr, sizeof(str_addr));
        TRACE2("Connecting to %s", str_addr);

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            fprintf(stderr, "Failed to connect to socket: (%d) %s\n", errno, strerror(errno));
            continue;
        }

        break;
    }

    if (p == 0) {
        fprintf(stderr, "Failed to connect to host %s@%s\n", global_conf->notification_host, portBuffer);
        return -1;
    }

    freeaddrinfo(servinfo);

    return sockfd;
}


bool send_final_notification() {

    int sockfd;
    long notification_size;
    JSON* json;

    /* Hard-restrict the size of the json we send to 4096 bytes. Matches the receive buffer
     * in the kronos executor */
    char json_buffer[4096];

    const GlobalConfig* global_conf = global_config_instance();

    if (!global_conf->enable_notifications) {
        return true;
    }

    if ((sockfd = open_notification_connection()) == -1) {
        fprintf(stderr, "Failed to send notification\n");
        return false;
    }

    /* Construct notification message */

    json = json_object_new();
    json_object_insert(json, "app", json_string_new("kronos-coordinator"));
    json_object_insert(json, "event", json_string_new("complete"));
    json_object_insert(json, "timestamp", json_number_new(take_time()));
    json_object_insert(json, "job_num", json_number_new(global_conf->job_num));

    notification_size = write_json_string(json_buffer, sizeof(json_buffer), json);
    free_json(json);

    /* Send message to the socket */

    TRACE1("Sending notification JSON");
    write(sockfd, json_buffer, notification_size);

    /* Clean up and exit */

    close(sockfd);
    return true;
}

/* ------------------------------------------------------------------------------------------------------------------ */
