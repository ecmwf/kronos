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
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
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
    fd_set fds;
    struct timeval timeout;
    int err;
    socklen_t err_len;

    void* addr;
    char str_addr[INET6_ADDRSTRLEN];
    const char* env;

    /* Default timeout of 60 seconds for notifications */

    long notification_timeout = 60;
    long l;

    env = getenv("KRONOS_NOTIFICATION_TIMEOUT");
    if (env) {
        l = strtol(env, NULL, 10);
        if (l > 1) {
            notification_timeout = l;
        } else {
            fprintf(stderr, "Invalid value for KRONOS_NOTIFICATION_TIMEOUT (%s). Using default: %li",
                    env, notification_timeout);
        }
    }

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

        /* Connect with timeout. Put the socket in non-blocking mode, and then select. */

        fcntl(sockfd, F_SETFL, O_NONBLOCK);

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1 && errno != EINPROGRESS) {
            close(sockfd);
            fprintf(stderr, "AAA Failed to connect to socket: (%d) %s\n", errno, strerror(errno));
            continue;
        }

        FD_ZERO(&fds);
        FD_SET(sockfd, &fds);
        timeout.tv_sec = notification_timeout;
        timeout.tv_usec = 0;

        switch (select(sockfd + 1, NULL, &fds, NULL, &timeout) == 1) {
        case 1:
            err_len = sizeof(err);
            getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &err, &err_len);
            if (err == 0) {
                /* Success! */
                break;
            }
            close(sockfd);
            fprintf(stderr, "Failed to connect to socket: (%d) %s\n", err, strerror(err));
            continue;
        case 0:
            close(sockfd);
            fprintf(stderr, "Failed to connect to socket due to timeout (t = %li s)\n", notification_timeout);
            continue;
        default:
            close(sockfd);
            fprintf(stderr, "Failed to connect to socket: (%d) %s\n", errno, strerror(errno));
            continue;
        };

        /*if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            fprintf(stderr, "Failed to connect to socket: (%d) %s\n", errno, strerror(errno));
            continue;
        }*/

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
    long notification_size, to_write, written;
    JSON* json;
    JSON* json_info;

    /* Hard-restrict the size of the json we send to 4096 bytes. Matches the receive buffer
     * in the kronos executor */
    char json_buffer[4096];

    const GlobalConfig* global_conf = global_config_instance();

    /* By default, we make 5 attempts */

    long notification_attempts = 5;
    long notification_retry_delay = 5;
    const char* kronos_token = "UNKNOWN";

    long l;
    long attempt;
    const char* p;


    if (!global_conf->enable_notifications || global_conf->mpi_rank != 0) {
        return true;
    }

    /* Environment variable to override the attempts */

    p = getenv("KRONOS_NOTIFICATION_ATTEMPTS");
    if (p) {
        l = strtol(p, NULL, 10);
        if (l > 1) {
            notification_attempts = l;
        } else {
            fprintf(stderr, "Invalid value for KRONOS_NOTIFICATION_ATTEMPTS (%s). Using default: %li",
                    p, notification_attempts);
        }
    }

    p = getenv("KRONOS_NOTIFICATION_RETRY_DELAY");
    if (p) {
        l = strtol(p, NULL, 10);
        if (l > 1) {
            notification_retry_delay = l;
        } else {
            fprintf(stderr, "Invalid value for KRONOS_NOTIFICATION_RETRY_DELAY (%s). Using default: %li",
                    p, notification_retry_delay);
        }
    }

    /* kronos-simulation hash string */
    p = getenv("KRONOS_TOKEN");
    if (p) {
        kronos_token = p;
    } else {
        fprintf(stderr, "KRONOS_TOKEN (%s). Using default: %s", p, kronos_token);
    }


    /* Construct notification message */
    json_info = json_object_new();
    json_object_insert(json_info, "app", json_string_new("kronos-synapp"));
    json_object_insert(json_info, "job", json_number_new(global_conf->job_num));
    json_object_insert(json_info, "timestamp", json_number_new(take_time()));

    json = json_object_new();
    json_object_insert(json, "type", json_string_new("Complete"));
    json_object_insert(json, "info", json_info);
    json_object_insert(json, "token", json_string_new(kronos_token));

    notification_size = write_json_string(json_buffer, sizeof(json_buffer), json);
    free_json(json);

    /* And we're go for the attempts */

    for (attempt = 0; attempt < notification_attempts; attempt++) {

        TRACE3("Sending notification JSON, attempt %li / %li", attempt + 1, notification_attempts);

        if ((sockfd = open_notification_connection()) == -1) {
            fprintf(stderr, "Failed to open notification connection\n");
            if (attempt < notification_attempts-1) {
                fprintf(stderr, "Sleeping for %li s ...\n", notification_retry_delay);
                sleep(notification_retry_delay);
            }
            continue;
        }

        /* Send message to the socket */

        TRACE1("Writing notification to socket");

        p = json_buffer;
        to_write = notification_size;
        while (to_write > 0) {
            if ((written = write(sockfd, p, to_write)) == -1) {
                fprintf(stderr, "Writing to socket failed: (%d) %s\n", errno, strerror(errno));
                break;
            }
            to_write -= written;
            p += written;
        }

        close(sockfd);

        /* success ? */
        if (written > 0) {
            TRACE1("Notification sent");
            return true;
        }
    }

    fprintf(stderr, "Sending notification failed\n");

    return false;
}

/* ------------------------------------------------------------------------------------------------------------------ */
