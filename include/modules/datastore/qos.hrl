%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by qos module.
%%%
%%% QoS management is based on two types of documents qos_entry and file_qos.
%%% See qos_entry.erl or file_qos.erl for more information.
%%%
%%% @end
%%%-------------------------------------------------------------------

-ifndef(QOS_HRL).
-define(QOS_HRL, 1).


-include_lib("ctool/include/time_series/common.hrl").


-define(QOS_SYNCHRONIZATION_PRIORITY, 224).

% macros used for operations on QoS expression
-define(OPERATORS, [<<"|">>, <<"&">>, <<"\\">>]).
-define(COMPARATORS, [<<"=">>, <<"<">>, <<">">>, <<">=">>, <<"<=">>]).

-define(L_PAREN, <<"(">>).
-define(R_PAREN, <<")">>).

-define(QOS_ANY_STORAGE, "anyStorage").

% macros used for operations on QoS bounded cache
-define(CACHE_TABLE_NAME(SpaceId),
    binary_to_atom(<<SpaceId/binary, "_qos_bounded_cache_table">>, utf8)).
-define(QOS_BOUNDED_CACHE_GROUP, <<"qos_bonded_cache_group">>).


% macros with QoS status
-define(IMPOSSIBLE_QOS_STATUS, impossible).
-define(PENDING_QOS_STATUS, pending).
-define(FULFILLED_QOS_STATUS, fulfilled).

% Macros representing directory type during QoS traverse. 
% Start directory is a directory, that traverse originated from (one per traverse),
% child directory is any other directory.
-define(QOS_STATUS_TRAVERSE_START_DIR, start_dir).
-define(QOS_STATUS_TRAVERSE_CHILD_DIR, child_dir).

% Request to remote providers to start QoS traverse.
% This record is used as an element of datastore document (qos_entry).
% Traverse is started in response to change of qos_entry document. (see qos_hooks.erl)
-record(qos_traverse_req, {
    % uuid of file that travers should start from
    % TODO: This field will be necessary after resolving VFS-5567. For now all
    % traverses starts from file/directory for which QoS has been added.
    start_file_uuid :: file_meta:uuid(),
    storage_id :: storage:id()
}).

% This record has the same fields as file_qos record (see file_qos.erl).
% The difference between this two is that file_qos stores information
% (in database) assigned to given file, whereas effective_file_qos is
% calculated using effective value mechanism and file_qos documents
% of the file and all its parents.
-record(effective_file_qos, {
    qos_entries = [] :: [qos_entry:id()],
    assigned_entries = #{} :: file_qos:assigned_entries(),
    in_trash = false :: boolean()
}).

-define(BYTES_STATS, <<"bytes">>).
-define(FILES_STATS, <<"files">>).

-define(QOS_TOTAL_TIME_SERIES_NAME, <<"total">>).
-define(QOS_STORAGE_TIME_SERIES_PREFIX_STR, "st_").
-define(QOS_STORAGE_TIME_SERIES_NAME(StorageId), <<?QOS_STORAGE_TIME_SERIES_PREFIX_STR, StorageId/binary>>).

-define(QOS_MINUTE_METRIC_NAME, <<"minute">>).
-define(QOS_HOUR_METRIC_NAME, <<"hour">>).
-define(QOS_DAY_METRIC_NAME, <<"day">>).
-define(QOS_MONTH_METRIC_NAME, <<"month">>).


-define(QOS_STATS_METRICS, #{
    ?QOS_MINUTE_METRIC_NAME => #metric_config{
        resolution = ?MINUTE_RESOLUTION,
        retention = 120,
        aggregator = sum
    },
    ?QOS_HOUR_METRIC_NAME => #metric_config{
        resolution = ?HOUR_RESOLUTION,
        retention = 48,
        aggregator = sum
    },
    ?QOS_DAY_METRIC_NAME => #metric_config{
        resolution = ?DAY_RESOLUTION,
        retention = 60,
        aggregator = sum
    },
    ?QOS_MONTH_METRIC_NAME => #metric_config{
        resolution = ?MONTH_RESOLUTION,
        retention = 12,
        aggregator = sum
    }
}).


-define(QOS_STATS_COLLECTION_SCHEMA(Unit), #time_series_collection_schema{time_series_schemas = [
    #time_series_schema{
        name_generator_type = exact,
        name_generator = ?QOS_TOTAL_TIME_SERIES_NAME,
        unit = Unit,
        metrics = ?QOS_STATS_METRICS
    },
    #time_series_schema{
        name_generator_type = add_prefix,
        name_generator = <<?QOS_STORAGE_TIME_SERIES_PREFIX_STR>>,
        unit = Unit,
        metrics = ?QOS_STATS_METRICS
    }
]}).
-define(QOS_BYTES_STATS_COLLECTION_SCHEMA, ?QOS_STATS_COLLECTION_SCHEMA(bytes)).
-define(QOS_FILES_STATS_COLLECTION_SCHEMA, ?QOS_STATS_COLLECTION_SCHEMA(none)).

-endif.
