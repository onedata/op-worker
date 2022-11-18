%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module providing utility function for readdir plus related file listing operations.
%%% @end
%%%--------------------------------------------------------------------
-module(readdir_plus).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").

-export([
    gather_attributes/4
]).

-type gather_attributes_fun(Entry, Attributes) :: 
    fun((user_ctx:ctx(), Entry, attr_req:compute_file_attr_opts()) -> Attributes).

-export_type([gather_attributes_fun/2]).

-define(MAX_MAP_CHILDREN_PROCESSES, application:get_env(
    ?APP_NAME, max_read_dir_plus_procs, 20
)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Calls GatherAttributesFun for every passed entry in parallel and
%% filters out entries for which it raised an error (potentially docs not
%% synchronized between providers or deleted files).
%% @end
%%--------------------------------------------------------------------
-spec gather_attributes(
    user_ctx:ctx(),
    gather_attributes_fun(Entry, Attributes),
    [Entry],
    attr_req:compute_file_attr_opts()
) ->
    [Attributes].
gather_attributes(UserCtx, GatherAttributesFun, Entries, BaseOpts) ->
    EntriesNum = length(Entries),
    EnumeratedChildren = lists_utils:enumerate(Entries),
    FilterMapFun = fun({Num, Entry}) ->
        try
            Result = case Num == 1 orelse Num == EntriesNum of
                true ->
                    GatherAttributesFun(UserCtx, Entry, BaseOpts#{
                        name_conflicts_resolution_policy => resolve_name_conflicts
                    });
                false ->
                    GatherAttributesFun(UserCtx, Entry, BaseOpts#{
                        name_conflicts_resolution_policy => allow_name_conflicts
                    })
            end,
            {true, Result}
        catch Class:Reason ->
            case datastore_runner:normalize_error(Reason) of
                not_found ->
                    % Entry metadata can be not fully synchronized with other provider
                    false;
                _ ->
                    erlang:apply(erlang, Class, [Reason])
            end
        end
    end,
    lists_utils:pfiltermap(FilterMapFun, EnumeratedChildren, ?MAX_MAP_CHILDREN_PROCESSES).
