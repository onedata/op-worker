%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module providing utility functions used across modules responsible for file listing.
%%% @end
%%%--------------------------------------------------------------------
-module(file_listing_utils).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").

-export([
    map_entries/4,
    extend_compute_attr_opts/2
]).

-type entry_type() :: edge_entry | inner_entry.

-export_type([entry_type/0]).

-define(MAX_MAP_CHILDREN_PROCESSES, application:get_env(
    ?APP_NAME, max_read_dir_plus_procs, 20
)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Calls MapFunctionInsecure for every passed entry in parallel and
%% filters out entries for which it raised error (potentially docs not
%% synchronized between providers or deleted files).
%% @end
%%--------------------------------------------------------------------
-spec map_entries(
    user_ctx:ctx(),
    MapFunInsecure :: fun((user_ctx:ctx(), Entry, BaseOpts, entry_type()) -> Term),
    Entries :: [Entry],
    BaseOpts
) ->
    [Term].
map_entries(UserCtx, MapFunInsecure, Entries, BaseOpts) ->
    EntriesNum = length(Entries),
    EnumeratedChildren = lists_utils:enumerate(Entries),
    FilterMapFun = fun({Num, Entry}) ->
        try
            Result = case Num == 1 orelse Num == EntriesNum of
                true ->
                    MapFunInsecure(UserCtx, Entry, BaseOpts, edge_entry);
                false ->
                    MapFunInsecure(UserCtx, Entry, BaseOpts, inner_entry)
            end,
            {true, Result}
        catch _:_ ->
            % Entry metadata can be not fully synchronized with other provider
            false
        end
    end,
    lists_utils:pfiltermap(FilterMapFun, EnumeratedChildren, ?MAX_MAP_CHILDREN_PROCESSES).


%%--------------------------------------------------------------------
%% @doc
%% Other files than first and last don't need to resolve name
%% conflicts (to check for collisions) as list_children
%% (file_meta_forest:tag_ambiguous to be precise) already did it
%% @end
%%--------------------------------------------------------------------
-spec extend_compute_attr_opts(attr_req:compute_file_attr_opts(), entry_type()) -> 
    attr_req:compute_file_attr_opts().
extend_compute_attr_opts(Opts, edge_entry) ->
    Opts#{name_conflicts_resolution_policy => resolve_name_conflicts};
extend_compute_attr_opts(Opts, inner_entry) ->
    Opts#{name_conflicts_resolution_policy => allow_name_conflicts}.
