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


-include("modules/datastore/qos.hrl").


% Get QoS ID from QoS name -> QoS ID mapping
-define(GET_QOS_ID(QOS_NAME, NAME_ID_MAPPING), maps:get(QOS_NAME, NAME_ID_MAPPING)).


% util names used in tests
-define(QOS1, <<"Qos1">>).
-define(QOS2, <<"Qos2">>).
-define(QOS3, <<"Qos3">>).

-define(P1, <<"p1">>).
-define(P2, <<"p2">>).
-define(P3, <<"p3">>).
-define(P4, <<"p4">>).


% record that holds information about QoS that should be added in test
-record(qos_to_add, {
    worker, % worker on which QoS will be added
    qos_name, % name of QoS - used only in tests to identify different QoS
    path, % path to file / directory for which QoS should be added
    expression, % QoS expression in infix notation
    replicas_num = 1 % number of required replicas
}).


% record that holds information about expected QoS entry
-record(expected_qos_entry, {
    workers, % list of workers on which check QoS entry
    qos_name, % name of QoS - used only in tests to identify different QoS

    % below fields correspond to fields of QoS entry record
    file_key, % either {path, Path} or {uuid, Uuid}. Will be transformed to Uuid.
    qos_expression_in_rpn,
    replicas_num,
    is_possible = true
}).


% record that holds information about expected file QoS
-record(expected_file_qos, {
    workers, % list of workers on which check file QoS
    path, % path to file or directory for which check file QoS

    % below files correspond to fields in file QoS record
    qos_entries,
    target_storages
}).


% record that holds information about test directory structure
% can be used either to define directory structure that should be created
% or to define file distribution that should be checked
-record(test_dir_structure, {
    worker, % worker on which create / assert directory structure
    assertion_workers,
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
    dir_structure
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
    initial_dir_structure,
    qos_to_add,
    wait_for_qos_fulfillment,
    expected_qos_entries = [],
    expected_file_qos = [],
    expected_dir_structure
}).


% record for specification of tests that checks effective QoS. Very similar to
% above fulfill_qos_test_spec, the only difference is that it allows to define
% effective QoS that should be checked instead of file_qos.
-record(effective_qos_test_spec, {
    initial_dir_structure,
    qos_to_add,
    wait_for_qos_fulfillment,
    expected_qos_entries = [],
    expected_effective_qos,
    expected_dir_structure
}).
