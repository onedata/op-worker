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
        #provider_response{
            provider_response = #recursive_listing_result{
                entries = Entries,
                pagination_token = NextPaginationToken
            }
        } = dir_req:list_recursively(UserCtx, RootFileCtx, ListOpts, []),

        ProgressMarker = case NextPaginationToken of
            undefined -> done;
            _ -> more
        end,

        Results = lists:map(fun({_Path, #file_attr{guid = FileGuid}}) ->
            {ok, file_ctx:new_by_guid(FileGuid)}
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
