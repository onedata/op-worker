%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains utility records and macros for QoS tests.
%%% @end
%%%-------------------------------------------------------------------
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
-include("modules/datastore/qos.hrl").


% util names used in tests
-define(QOS1, <<"Qos1">>).
-define(QOS2, <<"Qos2">>).
-define(QOS3, <<"Qos3">>).

-define(P1, <<"p1">>).
-define(P2, <<"p2">>).
-define(P3, <<"p3">>).


-define(filename(Name, Num), <<Name/binary,(integer_to_binary(Num))/binary>>).

-define(TEST_DATA, <<"test_data">>).

-type qos_name() :: binary().

% record that holds information about qos_entry that should be added in test
-record(qos_to_add, {
    provider :: od_provider:id(), % provider on which QoS will be added
    qos_name :: qos_name(), % name of QoS - used only in tests to identify different QoS
    path :: file_meta:path(), % path to file / directory for which QoS should be added
    expression :: qos_expression:infix(), % QoS expression in infix notation
    replicas_num = 1 :: qos_entry:replics_num() % number of required replicas
}).


% record that holds information about expected qos_entry
-record(expected_qos_entry, {
    providers :: [od_provider:id()], % list of providers on which check QoS entry
    qos_name :: qos_name(), % name of QoS - used only in tests to identify different QoS

    % below fields correspond to fields of QoS entry record
    file_key :: {path, file_meta:path()} | {uuid, file_meta:uuid()},
    qos_expression :: qos_entry:expression(),
    replicas_num :: qos_entry:replics_num(),
    possibility_check :: od_provider:id()
}).


% record that holds information about expected file_qos
-record(expected_file_qos, {
    providers :: [od_provider:id()], % list of providers on which check file QoS
    path :: file_meta:path(), % path to file or directory for which check file QoS

    % below files correspond to fields in file QoS record
    qos_entries :: [qos_entry:record()],
    assigned_entries :: file_qos:assigned_entries()
}).


% record that holds information about test directory structure
% can be used either to define directory structure that should be created
% or to define file distribution that should be checked
-record(test_dir_structure, {
    provider :: od_provider:id(), % provider on which create / assert directory structure
    assertion_providers :: [od_provider:id()],
    % directory structure
    % example:
    %%  {?SPACE1, [
    %%      {<<"dir1">>, [
    %%          {<<"dir2">>, [
    %%              {<<"file21">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1)]}
    %%          ]},
    %%          {<<"dir3">>, [
    %%              {<<"file31">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP3)]}
    %%          ]}
    %%      ]}
    %%  ]}
    dir_structure :: tuple()
}).

% record storing QoS specification for tests
-record(qos_spec, {
    qos_to_add :: [#qos_to_add{}],
    expected_qos_entries :: [#expected_qos_entry{}],
    expected_file_qos :: [#expected_file_qos{}],
    expected_effective_qos :: [#expected_file_qos{}]
}).

% record for specification of tests that performs following actions:
%   1. initial_dir_structure - creates directory structure (see test_dir_structure
%      record defined in this file for more information)
%   2. qos_to_add - list of QoS to add (see qos_to_add record defined in this file
%      for more information)
%   3. wait_for_qos_fulfillment - list of tuples {QosName, ListOfWorkers} that
%      allows to specify for which QoS fulfilment wait
%   4. expected_qos_entries - list of qos_entry documents to check (see
%      expected_qos_entry record defined in this file for more information)
%   5. expected_file_qos - list of file_qos documents to check (see
%      expected_file_qos record defined in this file for more information)
%   6. expected_dir_structure - checks file distribution see test_dir_structure
%      record defined in this file for more information)
-record(fulfill_qos_test_spec, {
    initial_dir_structure :: undefined | #test_dir_structure{},
    qos_to_add :: [#qos_to_add{}],
    wait_for_qos_fulfillment = true :: boolean(),
    expected_qos_entries = [] :: [#expected_qos_entry{}],
    expected_file_qos = [] :: [#expected_file_qos{}],
    expected_dir_structure :: undefined | #test_dir_structure{}
}).


% record for specification of tests that checks effective QoS. Very similar to
% above fulfill_qos_test_spec, the only difference is that it allows to define
% effective QoS that should be checked instead of file_qos.
-record(effective_qos_test_spec, {
    initial_dir_structure :: undefined | #test_dir_structure{},
    qos_to_add :: [#qos_to_add{}],
    expected_qos_entries = [] :: [#expected_qos_entry{}],
    expected_effective_qos :: [#expected_file_qos{}],
    expected_dir_structure :: undefined | #test_dir_structure{}
}).


-define(simple_dir_structure(SpaceId, Name, Distribution),
    {SpaceId, [
        {Name, ?TEST_DATA, Distribution}
    ]}
).
-define(nested_dir_structure(SpaceId, Name, Distribution),
    {SpaceId, [
        {Name, [
            {?filename(Name, 1), [
                {?filename(Name, 1), ?TEST_DATA, Distribution},
                {?filename(Name, 2), ?TEST_DATA, Distribution},
                {?filename(Name, 3), ?TEST_DATA, Distribution},
                {?filename(Name, 4), ?TEST_DATA, Distribution}
            ]},
            {?filename(Name, 2), [
                {?filename(Name, 1), ?TEST_DATA, Distribution},
                {?filename(Name, 2), ?TEST_DATA, Distribution},
                {?filename(Name, 3), ?TEST_DATA, Distribution},
                {?filename(Name, 4), ?TEST_DATA, Distribution}
            ]}
        ]}
    ]}
).
