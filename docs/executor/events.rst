
======
Events
======

When the executor is configured in "events" mode, jobs can be triggered not only upon completion of
a previous jobs, but also on various other signals including time and software-defined
notifications. Notifications are sent in JSON to the executor server and can be used as job
dependencies in the KSchedule.

Sending notifications to Kronos
===============================

The Kronos executor spins up a TCP server listening for notifications. In order to avoid collisions,
every run of the executor gets a unique token that needs to be provided to the server. The JSON
object communicated to the notification system should have at least the following properties:

========  ======  =================================================================
Property  Type    Description
========  ======  =================================================================
token     string  The simulation token
type      string  The notification type (see below)
info      object  The information relative to the event (depends on the event type)
========  ======  =================================================================

Additional properties may be needed depending on the type.

Specifying dependencies based on events
=======================================

The ``dependencies`` property of a job is a list of event descriptions with the same requirements as
the corresponding notification, except for the token that is not needed. When checking for triggered
dependencies, the event object and the dependency specification are compared. Depending on the event
type, different properties will need to match in order to trigger a subsequent job.

Events reference
================

``Complete``: job completion event
----------------------------------

This event signals the completion of a job. It is sent automatically by the synthetic application,
and external jobs need to send the notification. The event's ``info`` object contains the following
properties:

=========  =======  ==========  ==================================================
Property   Type     Must match  Description
=========  =======  ==========  ==================================================
app        string   Yes         Application type
job        integer  Yes         Job identifier (position in the job list)
timestamp  integer  No          Time at which the event completed (UNIX timestamp)
=========  =======  ==========  ==================================================

The value of ``app`` corresponding to the synthetic application is "kronos-synapp".

``Time``: timer event
---------------------

This event signals a point in time. It is automatically issued by the executor for jobs that need to
be executed at a specific point in time (relative to the start of the workflow). The ``info``
object contains only one property:

=========  ======  ==========  =================================================
Property   Type    Must match  Description
=========  ======  ==========  =================================================
timestamp  number  Yes         Time since the start of the workflow (in seconds)
=========  ======  ==========  =================================================

``NotifyMetadata`` and ``MetadataChange``: metadata events
----------------------------------------------------------

These events signals new metadata or a change in metadata. External jobs can send such notifications
at any time while running. The difference between the two types is only formal and they hold exactly
the same data. The ``info`` object contains the following properties:

========  =======  ==========  =========================================
Property  Type     Must match  Description
========  =======  ==========  =========================================
app       string   Yes         Application type
job       integer  Yes         Job identifier (position in the job list)
job_name  string   No          Name of the job
========  =======  ==========  =========================================

Additionally, the event object should have a ``metadata`` property. Any property in this object
has to match when triggering dependencies.

