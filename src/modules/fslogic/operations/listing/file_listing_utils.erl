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

-export([map_children_to_attrs/5]).

-define(MAX_MAP_CHILDREN_PROCESSES, application:get_env(
    ?APP_NAME, max_read_dir_plus_procs, 20
)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Calls MapFunctionInsecure for every passed children in parallel and
%% filters out children for which it raised error (potentially docs not
%% synchronized between providers or deleted files).
%% @end
%%--------------------------------------------------------------------
-spec map_children_to_attrs(
    user_ctx:ctx(),
    MapFunInsecure :: fun((user_ctx:ctx(), file_ctx:ctx(), attr_req:compute_file_attr_opts()) -> Term),
    Children :: [file_ctx:ctx()],
    IncludeReplicationStatus :: boolean(),
    IncludeLinkCount :: boolean()
) ->
    [Term].
map_children_to_attrs(UserCtx, MapFunInsecure, Children, IncludeReplicationStatus, IncludeLinkCount) ->
    ChildrenNum = length(Children),
    EnumeratedChildren = lists_utils:enumerate(Children),
    Opts = #{
        allow_deleted_files => false,
        include_size => true
    },
    FilterMapFun = fun({Num, ChildCtx}) ->
        try
            Result = case Num == 1 orelse Num == ChildrenNum of
                true ->
                    MapFunInsecure(UserCtx, ChildCtx, Opts#{
                        name_conflicts_resolution_policy => resolve_name_conflicts,
                        include_replication_status => IncludeReplicationStatus,
                        include_link_count => IncludeLinkCount
                    });
                false ->
                    % Other files than first and last don't need to resolve name
                    % conflicts (to check for collisions) as list_children
                    % (file_meta_forest:tag_ambiguous to be precise) already did it
                    MapFunInsecure(UserCtx, ChildCtx, Opts#{
                        name_conflicts_resolution_policy => allow_name_conflicts,
                        include_replication_status => IncludeReplicationStatus,
                        include_link_count => IncludeLinkCount
                    })
            end,
            {true, Result}
        catch _:_ ->
            % File metadata can be not fully synchronized with other provider
            false
        end
    end,
    lists_utils:pfiltermap(FilterMapFun, EnumeratedChildren, ?MAX_MAP_CHILDREN_PROCESSES).

