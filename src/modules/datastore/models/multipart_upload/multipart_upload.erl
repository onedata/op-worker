%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% Module implementing model for storing multipart uploads. 
%%% Listed uploads are ordered by path and then creation time (multiple uploads can exist for
%%% the same path).
%%% @end
%%%-------------------------------------------------------------------
-module(multipart_upload).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% functions operating on document using datastore model API
-export([create/3, get/1, finish/2, list/4]).

-export([encode_token/1, decode_token/1, is_last/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-define(CTX, #{
    model => ?MODULE
}).

-type id() :: datastore_model:key().
-type path() :: binary().
-type pagination_token() :: #{
    token => datastore_links_iter:token(),
    prev_link_name => datastore_links:link_name(),
    is_last => boolean()
}.
-type record() :: #multipart_upload{}.

-export_type([id/0, path/0, pagination_token/0, record/0]).

-compile({no_auto_import, [get/1]}).


%%%===================================================================
%%% API
%%%===================================================================

-spec create(od_space:id(), od_user:id(), file_meta:path()) -> record().
create(SpaceId, UserId, Path) ->
    Timestamp = global_clock:timestamp_millis(),
    Scope = get_upload_scope(SpaceId, UserId),
    LinkKey = build_link_key(Path, Timestamp),
    Record = #multipart_upload{path = Path, creation_time = Timestamp, space_id = SpaceId},
    {ok, #document{key = UploadId}} = datastore_model:create(?CTX, #document{value = Record}),
    ok = ?extract_ok(datastore_model:add_links(?CTX, Scope, oneprovider:get_id(), {LinkKey, UploadId})),
    Record#multipart_upload{multipart_upload_id = UploadId}.


-spec finish(od_user:id(), id()) -> ok | {error, term()}.
finish(UserId, UploadId) ->
    case get(UploadId) of
        {ok, #document{value = #multipart_upload{path = Path, creation_time = CreationTime, space_id = SpaceId}}} ->
            LinkKey = build_link_key(Path, CreationTime),
            Scope = get_upload_scope(SpaceId, UserId),
            ok = datastore_model:delete(?CTX, UploadId),
            ok = multipart_upload_part:cleanup(UploadId),
            ok = datastore_model:delete_links(?CTX, Scope, oneprovider:get_id(), LinkKey);
        {error, _} = Error ->
            Error
    end.


-spec get(id()) -> {ok, datastore_doc:doc(record())} | {error, term()}.
get(UploadId) ->
    datastore_model:get(?CTX, UploadId).


-spec list(od_space:id(), od_user:id(), non_neg_integer(), pagination_token()) -> 
    {ok, [record()], pagination_token() | undefined}.
list(SpaceId, UserId, Limit, PaginationToken) ->
    BaseOpts = case PaginationToken of
        undefined -> #{token => #link_token{}};
        _ -> maps:remove(is_last, PaginationToken)
    end,
    FoldFun = fun(#link{name = PathAndTimestamp, target = UploadId}, Acc) -> 
        {ok, [{UploadId, PathAndTimestamp} | Acc]} 
    end,
    Opts = BaseOpts#{
        size => Limit,
        prev_tree_id => oneprovider:get_id() % required for exclusive listing
    },
    {{ok, ReversedLinks}, DatastoreToken} = datastore_model:fold_links(
        ?CTX, get_upload_scope(SpaceId, UserId), oneprovider:get_id(), FoldFun, [], Opts),
    NextToken = case {ReversedLinks, DatastoreToken#link_token.is_last} of
        {[{_, LastLinkName} | _], false} ->
            #{
                token => DatastoreToken,
                prev_link_name => LastLinkName,
                is_last => false
            };
        _ ->
            #{
                is_last => true
            }
    end,
    FinalList = lists:foldl(fun({UploadId, LinkKey}, Acc) ->
        {Path, CreationTime} = split_link_key(LinkKey),
        [#multipart_upload{
            multipart_upload_id = UploadId,
            path = Path,
            creation_time = CreationTime
        } | Acc]
    end, [], ReversedLinks),
    {ok, FinalList, NextToken}.


-spec encode_token(pagination_token() | undefined) -> binary() | undefined.
encode_token(undefined) ->
    undefined;
encode_token(Token) ->
    base64url:encode(term_to_binary(Token)).


-spec decode_token(binary() | undefined) -> pagination_token() | undefined.
decode_token(undefined) ->
    undefined;
decode_token(EncodedToken) ->
   binary_to_term(base64url:decode(EncodedToken)).


-spec is_last(pagination_token()) -> boolean().
is_last(Token) ->
    maps:get(is_last, Token).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec build_link_key(file_meta:path(), time:millis()) -> binary().
build_link_key(Path, Timestamp) ->
    <<Path/binary, "/", (integer_to_binary(Timestamp))/binary>>.


-spec split_link_key(binary()) -> {file_meta:path(), time:millis()}.
split_link_key(LinkKey) ->
    Path = filename:dirname(LinkKey),
    Timestamp = binary_to_integer(filename:basename(LinkKey)),
    {Path, Timestamp}.


-spec get_upload_scope(od_space:id(), od_user:id()) -> binary().
get_upload_scope(SpaceId, UserId) ->
    datastore_key:new_from_digest([SpaceId, UserId]).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {multipart_upload_id, binary},
        {path, binary},
        {creation_time, integer},
        {space_id, binary}
    ]}.
