%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides `transfer_iterator` functionality for file tree.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_file_tree_iterator).
-author("Bartosz Walkowicz").

-behaviour(transfer_iterator).

-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    new/2,
    get_next_batch/3
]).

-record(transfer_file_tree_iterator, {
    transfer_id :: transfer:id(),
    root_file_ctx :: file_ctx:ctx(),
    pagination_token :: undefined | recursive_listing:pagination_token()
}).
-type instance() :: #transfer_file_tree_iterator{}.

-export_type([instance/0]).

% List of files and directories directly in transfer root directory that will be ignored during transfer.
-define(IGNORED_TOP_FILES, op_worker:get_env(ignored_top_files, [])).


%%%===================================================================
%%% API
%%%===================================================================


-spec new(transfer:id(), file_ctx:ctx()) -> instance().
new(TransferId, RootFileCtx) ->
    #transfer_file_tree_iterator{
        transfer_id = TransferId,
        root_file_ctx = RootFileCtx,
        pagination_token = undefined
    }.


-spec get_next_batch(user_ctx:ctx(), pos_integer(), instance()) ->
    {more | done, [{ok, file_ctx:ctx()}], instance()} |
    {error, term()}.
get_next_batch(UserCtx, Limit, Iterator = #transfer_file_tree_iterator{
    transfer_id = TransferId,
    root_file_ctx = RootFileCtx,
    pagination_token = PaginationToken
}) ->
    ListOpts = maps_utils:remove_undefined(#{
        limit => Limit,
        pagination_token => PaginationToken,
        include_directories => false
    }),

    try
        #fuse_response{
            fuse_response = #recursive_listing_result{
                entries = Entries,
                pagination_token = NextPaginationToken
            }
        } = dir_req:list_recursively(UserCtx, RootFileCtx, ListOpts, []),

        ProgressMarker = case NextPaginationToken of
            undefined -> done;
            _ -> more
        end,

        Results = lists:filtermap(fun({Path, #file_attr{guid = FileGuid}}) ->
            case filepath_utils:split(Path) of
                [PathToken | _] ->
                    case lists:member(PathToken, ?IGNORED_TOP_FILES) of
                        true -> false;
                        false -> {true, {ok, file_ctx:new_by_guid(FileGuid)}}
                    end;
                _ ->
                    {true, {ok, file_ctx:new_by_guid(FileGuid)}}
            end
        end, Entries),

        NewIterator = Iterator#transfer_file_tree_iterator{
            pagination_token = NextPaginationToken
        },

        {ProgressMarker, Results, NewIterator}

    catch _:Reason:Stacktrace ->
        ?error_stacktrace(
            "Recursive listing failed due to ~p when processing transfer ~p",
            [Reason, TransferId],
            Stacktrace
        ),
        {error, datastore_runner:normalize_error(Reason)}
    end.
