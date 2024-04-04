%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains macros and modules inclusions used by
%%% harvesting tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(HARVESTING_TEST_HRL).
-define(HARVESTING_TEST_HRL, 1).

-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/harvesting/harvesting.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/http/codes.hrl").

-define(SPACE_ID1, <<"space_id1">>).
-define(SPACE_ID2, <<"space_id2">>).
-define(SPACE_ID3, <<"space_id3">>).
-define(SPACE_ID4, <<"space_id4">>).
-define(SPACE_ID5, <<"space_id5">>).

-define(SPACE_NAME1, <<"space1">>).
-define(SPACE_NAME2, <<"space2">>).
-define(SPACE_NAME3, <<"space3">>).
-define(SPACE_NAME4, <<"space4">>).
-define(SPACE_NAME5, <<"space5">>).

-define(SPACE_NAMES, #{
    ?SPACE_ID1 => ?SPACE_NAME1,
    ?SPACE_ID2 => ?SPACE_NAME2,
    ?SPACE_ID3 => ?SPACE_NAME3,
    ?SPACE_ID4 => ?SPACE_NAME4,
    ?SPACE_ID5 => ?SPACE_NAME5
}).

-define(SPACE_NAME(__SpaceId), maps:get(__SpaceId, ?SPACE_NAMES)).
-define(SPACE_IDS, maps:keys(?SPACE_NAMES)).

-define(PATH(FileName, SpaceId), filename:join(["/", ?SPACE_NAME(SpaceId), FileName])).

-define(HARVESTER1, <<"harvester1">>).
-define(HARVESTER2, <<"harvester2">>).
-define(HARVESTER3, <<"harvester3">>).

-define(USER_ID, <<"user1">>).
-define(SESS_ID(Worker),
    ?config({session_id, {?USER_ID, ?GET_DOMAIN(Worker)}}, Config)).

-define(XATTR_NAME, <<"xattr_name_", (?RAND_NAME)/binary>>).
-define(XATTR_VAL, <<"xattr_val_", (?RAND_NAME)/binary>>).

-define(DIR_NAME, <<"dir", (?RAND_NAME)/binary>>).
-define(FILE_NAME, <<"file_", (?RAND_NAME)/binary>>).

-define(RAND_NAME,
    <<(str_utils:to_binary(?FUNCTION))/binary, "_", (integer_to_binary(rand:uniform(?RAND_RANGE)))/binary>>).

-define(RAND_RANGE, 1000000000).
-define(ATTEMPTS, 30).

-define(PROVIDER_ID(Node), rpc:call(Node, oneprovider, get_id, [])).

%% Test config:
%% space_id1:
%%  * supported by: p1
%%  * harvesters: harvester1
%%  * indices: index1
%% space_id2:
%%  * supported by: p1
%%  * harvesters: harvester1, harvester2
%% space_id3:
%%  * supported by: p1
%%  * harvesters: harvester1
%% space_id4:
%%  * supported by: p1
%%  * harvesters: harvester1
%% space_id5:
%%  * supported by: p1, p2
%%  * harvesters: harvester3

-define(HARVEST_METADATA, harvest_metadata).
-define(HARVEST_METADATA(SpaceId, Destination, Batch, ExpProviderId),
    {?HARVEST_METADATA, SpaceId, Destination, Batch, ExpProviderId}).

%% NOTE!!!
%% This assert assumes that list ExpBatch is sorted in the order of increasing sequences
-define(assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId),
    ?assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, ?ATTEMPTS)).
-define(assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, Timeout),
    (
        (fun
            AssertFun(__SpaceId, __Destination, [], __Unexpected, __ProviderId, __Timeout) ->
                % all expected changes has been received
                % resend these entries that were unexpected
                self() ! ?HARVEST_METADATA(__SpaceId, __Destination, __Unexpected, __ProviderId),
                ok;
            AssertFun(__SpaceId, __Destination, __Batch, __Unexpected, __ProviderId, __Timeout) ->
                __TimeoutInMillis = timer:seconds(__Timeout),
                receive
                    ?HARVEST_METADATA(
                        __SpaceId,
                        __Destination,
                        __ReceivedBatch,
                        __ProviderId
                    ) ->
                        {__ExpBatchLeft, __NewUnexpected} = harvesting_test_utils:subtract_batches(__Batch, __ReceivedBatch),
                        AssertFun(__SpaceId, __Destination, __ExpBatchLeft, __Unexpected ++ __NewUnexpected,
                            __ProviderId, __Timeout)
                after
                    __TimeoutInMillis ->
                        __Args = [{module, ?MODULE},
                            {line, ?LINE},
                            {expected, {__SpaceId, __Destination, __Batch, __ProviderId, __Timeout}},
                            {value, timeout}],
                        ct:print("assertReceivedHarvestMetadata_failed: ~tp~n", [__Args]),
                        erlang:error({assertReceivedHarvestMetadata_failed, __Args})
                end
        end)(ExpSpaceId, ExpDestination, ExpBatch, [], ExpProviderId, Timeout)
    )).

-define(assertNotReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId),
    ?assertNotReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, ?ATTEMPTS)).
-define(assertNotReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, Timeout),
    (
        (fun AssertFun(__SpaceId, __Destination, __Batch, __Unexpected, __ProviderId, __Timeout) ->
            Stopwatch = stopwatch:start(),
            __TimeoutInMillis = timer:seconds(__Timeout),
            receive
                __HM = ?HARVEST_METADATA(
                    __SpaceId,
                    __Destination,
                    __ReceivedBatch,
                    __ProviderId
                ) ->
                    ElapsedTime = stopwatch:read_seconds(Stopwatch),
                    {__Batch2, __NewUnexpected} = harvesting_test_utils:subtract_batches(__Batch, __ReceivedBatch),
                    case length(__Batch2) < length(__Batch) of
                        false ->
                            AssertFun(__SpaceId, __Destination, __Batch, __Unexpected ++ __NewUnexpected,
                                __ProviderId, max(__Timeout - ElapsedTime, 0));
                        true ->
                            % __Batch2 is smaller than __Batch which means that one of changes, which was
                            % expected not to occur, actually occurred
                            __Args = [
                                {module, ?MODULE},
                                {line, ?LINE}
                            ],
                            ct:print("assertNotReceivedHarvestMetadata_failed: ~lp~n"
                                "Unexpectedly received: ~tp~n", [__Args, __HM]),
                            erlang:error({assertNotReceivedHarvestMetadata_failed, __Args})
                    end
            after
                __TimeoutInMillis ->
                    % resend these entries that were unexpected
                    self() ! ?HARVEST_METADATA(__SpaceId, __Destination, __Unexpected, __ProviderId),
                    ok
            end
        end)(ExpSpaceId, ExpDestination, ExpBatch, [], ExpProviderId, Timeout)
    )).

-define(INDEX(N), <<"index", (integer_to_binary(N))/binary>>).
-define(INDEX11, <<"index11">>).
-define(INDEX21, <<"index21">>).
-define(INDEX22, <<"index22">>).
-define(INDEX23, <<"index23">>).
-define(INDEX31, <<"index31">>).

-define(DUMMY_RDF, <<"dummy rdf">>).
-define(DUMMY_RDF(Content),
    <<(?DUMMY_RDF)/binary, "_", (str_utils:to_binary(Content))/binary>>).

-define(MOCK_HARVEST_METADATA_FAILURE, mock_harvest_metadata_failure).

-endif.