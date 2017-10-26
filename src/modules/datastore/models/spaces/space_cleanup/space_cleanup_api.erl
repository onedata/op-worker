%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Cleanup of unpopular files.
%%% @end
%%%--------------------------------------------------------------------
-module(space_cleanup_api).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([cleanup_space/6]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cleanups unpopular files from space
%% @end
%%--------------------------------------------------------------------
-spec cleanup_space(od_space:id(), autocleaning:id(), non_neg_integer(),
    non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok.
cleanup_space(SpaceId, AutocleaningId, SizeLowerLimit, SizeUpperLimit, MaxInactive, Target) ->

    FilesToClean = file_popularity_view:get_unpopular_files(
        SpaceId, SizeLowerLimit, SizeUpperLimit,  MaxInactive, null,
        null, null, null
    ),
    ConditionFun = fun() ->
        CurrentSize = space_quota:current_size(SpaceId),
        CurrentSize =< Target
    end,
    ForeachFun = fun(FileCtx) -> cleanup_replica(FileCtx, AutocleaningId) end,

    foreach_until(ForeachFun, ConditionFun, FilesToClean),
    autocleaning_controller:stop().

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Executes Fun for each element of the List until Condition is satisfied.
%% @end
%%-------------------------------------------------------------------
-spec foreach_until(fun((FileCtx :: file_ctx:ctx()) -> ok),
    fun(() -> boolean()), [file_ctx:ctx()]) -> ok.
foreach_until(Fun, Condition, List) ->
    lists:foldl(fun
        (_Arg, true) ->
            true;
        (Arg, false) ->
            Fun(Arg),
            Condition()
    end, false, List),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Invalidates local replica of given file
%% @end
%%--------------------------------------------------------------------
-spec cleanup_replica(file_ctx:ctx(), autocleaning:id()) -> ok.
cleanup_replica(FileCtx, AutocleaningId) ->
    RootUserCtx = user_ctx:new(session:root_session_id()),
    try
        #provider_response{status = #status{code = ?OK}} =
            sync_req:invalidate_file_replica(RootUserCtx, FileCtx,
                undefined, undefined, AutocleaningId),
        ok
    catch
        _Error:Reason ->
            ?error_stacktrace("Error of autocleaning procedure ~p due to ~p",
                [AutocleaningId, Reason]
            )
    end.