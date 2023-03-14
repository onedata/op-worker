%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for providing a higher level API for listing files. It translates 
%%% listing parameters used commonly in op-worker (options()) to datastore links parameters (datastore_options()). 
%%% Therefore it operates on two types of tokens used for continuous listing of directory content: 
%%%     * pagination_token - used by higher-level modules and passed to the external clients as an
%%%                          opaque string so that they can resume listing just after previously
%%%                          listed batch (results paging). It encodes a datastore_token and additional
%%%                          information about last position in the file tree, in case the datastore
%%%                          token expires. Hence, this token does not expire and always guarantees
%%%                          correct listing resumption.
%%%                    
%%%     * datastore_list_token - token used internally by datastore, has limited TTL. After its expiration, 
%%%                              information about last position in the file tree can be used to determine
%%%                              the starting point for listing. This token offers the best performance 
%%%                              when resuming listing from a certain point. 
%%% 
%%% When starting a new listing, the `tune_for_large_continuous_listing` parameter must be provided. 
%%% If the optimization is used, there is no guarantee that changes on file tree performed 
%%% after the start of first listing will be included. Therefore it shouldn't be used when 
%%% listing result is required to be up to date with state of file tree at the moment of listing 
%%% or when listed batch processing time is substantial (cache timeout is adjusted by cluster_worker's 
%%% `fold_cache_timeout` env variable).
%%% @end
%%%-------------------------------------------------------------------
-module(file_listing).
-author("Michal Stanisz").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/file_meta_forest.hrl").

-export([list/2]).
-export([build_index/1, build_index/2]).
-export([encode_pagination_token/1, decode_pagination_token/1]). 
-export([encode_index/1, decode_index/1]).
-export([is_finished/1, get_last_listed_filename/1]).
-export([infer_pagination_token/3]).

-record(list_index, {
    file_name :: file_meta:name(),
    tree_id :: file_meta_forest:tree_id() | undefined
}).

-type progress_marker() :: more | done.

-record(pagination_token, {
    last_index :: undefined | index(),
    datastore_token :: undefined | datastore_list_token(),
    progress_marker = more :: progress_marker()
}).

-opaque index() :: #list_index{}.
-opaque pagination_token() :: #pagination_token{}.
-type datastore_list_token() :: binary().
-type offset() :: integer().
-type limit() :: non_neg_integer().
-type whitelist() :: [file_meta:name()].

%% @formatter:off
-type options() :: #{
    pagination_token := pagination_token() | undefined,
    whitelist => undefined | whitelist(),
    handle_interrupted_call => boolean(), % default: true
    limit => limit()
} | #{
    tune_for_large_continuous_listing := boolean(),
    index => index(),
    offset => offset(),
    inclusive => boolean(),
    whitelist => undefined | whitelist(),
    handle_interrupted_call => boolean(), % default: true
    limit => limit()
}.
%% @formatter:on

-type datastore_list_opts() :: file_meta_forest:list_opts().
-type encoded_pagination_token() :: binary().
-type entry() :: file_meta_forest:link().

-export_type([offset/0, limit/0, index/0, pagination_token/0, options/0]).

-define(DEFAULT_LS_BATCH_LIMIT, op_worker:get_env(default_ls_batch_limit, 5000)).

%%%===================================================================
%%% API
%%%===================================================================

-spec list(file_meta:uuid(), options()) -> {ok, [entry()], pagination_token()} | {error, term()}.
list(FileUuid, ListOpts) ->
    check_exclusive_options(maps_utils:remove_undefined(ListOpts)),
    case maps:find(whitelist, ListOpts) of
        {ok, Whitelist} when Whitelist =/= undefined ->
            list_internal(list_whitelisted, FileUuid, ListOpts, [Whitelist]);
        _ ->
            list_internal(list, FileUuid, ListOpts, [])
    end.


-spec build_index(file_meta:name() | undefined) -> index().
build_index(Name) ->
    build_index(Name, undefined).


-spec build_index(file_meta:name() | undefined, file_meta_forest:tree_id() | undefined) -> index().
build_index(undefined, _) ->
    undefined;
build_index(Name, TreeId) when is_binary(Name) andalso (is_binary(TreeId) orelse TreeId == undefined) ->
    #list_index{file_name = Name, tree_id = TreeId};
build_index(_, _) ->
    %% TODO VFS-7208 uncomment after introducing API errors to fslogic
    %% throw(?ERROR_BAD_VALUE_BINARY())
    throw(?EINVAL).


-spec encode_pagination_token(pagination_token()) -> encoded_pagination_token().
encode_pagination_token(PaginationToken) ->
    mochiweb_base64url:encode(term_to_binary(PaginationToken)).


-spec decode_pagination_token(encoded_pagination_token()) -> pagination_token().
decode_pagination_token(Token) ->
    try binary_to_term(mochiweb_base64url:decode(Token)) of
        #pagination_token{} = PaginationToken ->
            PaginationToken;
        _ ->
            throw(?EINVAL)
    catch _:_ ->
        throw(?EINVAL)
    end.


-spec encode_index(index()) -> binary().
encode_index(#list_index{} = Index) ->
    mochiweb_base64url:encode(term_to_binary(Index)).


-spec decode_index(binary()) -> index().
decode_index(EncodedIndex) ->
    try binary_to_term(mochiweb_base64url:decode(EncodedIndex)) of
        #list_index{} = Index ->
            Index;
        _ ->
            throw(?EINVAL)
    catch _:_ ->
        #list_index{file_name = EncodedIndex}
    end.


-spec is_finished(pagination_token()) -> boolean().
is_finished(#pagination_token{progress_marker = ProgressMarker}) -> ProgressMarker == done.


-spec get_last_listed_filename(pagination_token()) -> file_meta:name() | undefined.
get_last_listed_filename(#pagination_token{last_index = #list_index{file_name = Name}}) -> 
    Name;
get_last_listed_filename(#pagination_token{last_index = undefined}) -> 
    undefined.


-spec infer_pagination_token([any()], file_meta:name() | undefined, limit()) -> 
    pagination_token().
infer_pagination_token(Result, LastName, Limit) when length(Result) < Limit ->
    #pagination_token{progress_marker = done, last_index = build_index(LastName)};
infer_pagination_token(_Result, LastName, _Limit) ->
    #pagination_token{progress_marker = more, last_index = build_index(LastName)}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec list_internal(atom(), file_meta:uuid(), options(), [any()]) -> 
    {ok, [entry()], pagination_token()} | {error, term()}.
list_internal(_FunctionName, _FileUuid, #{limit := 0, pagination_token := PaginationToken}, _ExtraArgs) ->
    {ok, [], PaginationToken};
list_internal(_FunctionName, _FileUuid, #{pagination_token := #pagination_token{progress_marker = done}} = PaginationToken, _ExtraArgs) ->
    {ok, [], PaginationToken};
list_internal(_FunctionName, _FileUuid, #{limit := 0} = ListOpts, _ExtraArgs) ->
    {ok, [], #pagination_token{
        last_index = maps:get(index, ListOpts, undefined),
        datastore_token = maps:get(token, convert_to_datastore_options(ListOpts), undefined),
        progress_marker = more
    }};
list_internal(FunctionName, FileUuid, ListOpts, ExtraArgs) ->
    ?run(begin
        DatastoreListOpts = convert_to_datastore_options(ListOpts),
        case erlang:apply(file_meta_forest, FunctionName, [FileUuid, DatastoreListOpts | ExtraArgs]) of
            {ok, Result, ExtendedInfo} ->
                {ok, Result, datastore_info_to_pagination_token(ExtendedInfo)};
            {error, _} = Error ->
                Error
        end
    end).


%% @private
-spec check_exclusive_options(options()) -> ok | no_return().
check_exclusive_options(#{pagination_token := _} = Options) ->
    maps_utils:is_empty(maps:with([tune_for_large_continuous_listing, index, offset, inclusive], Options)) orelse
        %% TODO VFS-7208 introduce conflicting options error after introducing API errors to fslogic
        throw(?EINVAL),
    ok;
check_exclusive_options(_Options) ->
    ok.


%% @private
-spec convert_to_datastore_options(options()) -> datastore_list_opts().
convert_to_datastore_options(#{pagination_token := PaginationToken} = Opts) ->
    #pagination_token{
        last_index = Index,
        datastore_token = DatastoreToken
    } = PaginationToken,
    BaseOpts = index_to_datastore_list_opts(Index),
    maps_utils:remove_undefined(BaseOpts#{
        token => DatastoreToken,
        handle_interrupted_call => sanitize_interrupted_call_opt(maps:get(handle_interrupted_call, Opts, undefined)),
        size => sanitize_limit(maps:get(limit, Opts, undefined))
    });
convert_to_datastore_options(Opts) ->
    BaseOpts = index_to_datastore_list_opts(maps:get(index, Opts, undefined)),
    DatastoreToken = case maps:find(tune_for_large_continuous_listing, Opts) of
        {ok, true} -> 
            #link_token{};
        {ok, false} -> 
            undefined;
        error ->
            %% TODO VFS-7208 uncomment after introducing API errors to fslogic
            %% throw(?ERROR_MISSING_REQUIRED_VALUE(tune_for_large_continuous_listing)),
            throw(?EINVAL)
    end,
    maps_utils:remove_undefined(BaseOpts#{
        size => sanitize_limit(
            maps:get(limit, Opts, undefined)),
        offset => sanitize_offset(
            maps:get(offset, Opts, undefined), 
            maps:get(prev_link_name, BaseOpts, undefined),
            maps:get(whitelist, Opts, undefined)),
        inclusive => sanitize_inclusive(
            maps:get(inclusive, Opts, undefined)),
        handle_interrupted_call => sanitize_interrupted_call_opt(
            maps:get(handle_interrupted_call, Opts, undefined)),
        token => DatastoreToken
    }).


%% @private
-spec sanitize_limit(limit() | undefined | any()) -> limit().
sanitize_limit(undefined) ->
    ?DEFAULT_LS_BATCH_LIMIT;
sanitize_limit(Limit) when is_integer(Limit) andalso Limit >= 0 ->
    Limit;
%% TODO VFS-7208 uncomment after introducing API errors to fslogic
%%sanitize_limit(Limit) when is_integer(Limit) ->
%%     throw(?ERROR_BAD_VALUE_TOO_LOW(Limit, 0));
sanitize_limit(_) ->
    %% TODO VFS-7208 uncomment after introducing API errors to fslogic
    %% throw(?ERROR_BAD_VALUE_INTEGER(size))
    throw(?EINVAL).


%% @private
-spec sanitize_inclusive(boolean() | undefined | any()) -> boolean().
sanitize_inclusive(undefined) ->
    false;
sanitize_inclusive(Inclusive) when is_boolean(Inclusive) ->
    Inclusive;
sanitize_inclusive(_) ->
    %% TODO VFS-7208 uncomment after introducing API errors to fslogic
    %% throw(?ERROR_BAD_VALUE_BOOLEAN(inclusive))
    throw(?EINVAL).


%% @private
-spec sanitize_offset(offset() | undefined | any(), file_meta_forest:last_name(), whitelist()) -> 
    offset() | undefined.
sanitize_offset(undefined, _PrevLinkName, _Whitelist) ->
    undefined;
sanitize_offset(Offset, _PrevLinkName, _Whitelist) when not is_integer(Offset) ->
    %% TODO VFS-7208 uncomment after introducing API errors to fslogic
    %% throw(?ERROR_BAD_VALUE_INTEGER(Offset))
    throw(?EINVAL);
sanitize_offset(Offset, undefined = _PrevLinkName, _Whitelist) ->
    % if prev_link_name is undefined, offset cannot be negative
    %% TODO VFS-7208 uncomment after introducing API errors to fslogic
    %% throw(?ERROR_BAD_VALUE_TOO_LOW(Offset, 0));
    Offset < 0 andalso throw(?EINVAL),
    Offset;
sanitize_offset(Offset, _, undefined = _Whitelist) ->
    % if whitelist is not provided, offset can be negative
    Offset;
sanitize_offset(Offset, _, _Whitelist) ->
    % if whitelist is provided, offset cannot be negative
    %% TODO VFS-7208 uncomment after introducing API errors to fslogic
    %% throw(?ERROR_BAD_VALUE_TOO_LOW(Offset, 0));
    Offset < 0 andalso throw(?EINVAL),
    Offset.


%% @private
-spec sanitize_interrupted_call_opt(boolean() | any()) -> boolean() | undefined.
sanitize_interrupted_call_opt(Boolean) when is_boolean(Boolean)->
    Boolean;
sanitize_interrupted_call_opt(undefined) ->
    undefined;
sanitize_interrupted_call_opt(_) ->
    %% TODO VFS-7208 uncomment after introducing API errors to fslogic
    %% throw(?ERROR_BAD_VALUE_BOOLEAN(handle_interrupted_call))
    throw(?EINVAL).


%% @private
-spec index_to_datastore_list_opts(index() | undefined | binary() | any()) -> map() | no_return().
index_to_datastore_list_opts(undefined) ->
    #{};
index_to_datastore_list_opts(#list_index{file_name = Name, tree_id = TreeId}) ->
    #{
        prev_link_name => Name,
        prev_tree_id => TreeId
    };
index_to_datastore_list_opts(_) ->
    %% TODO VFS-7208 uncomment after introducing API errors to fslogic
    %% throw(?ERROR_BAD_VALUE_INDEX())
    throw(?EINVAL).


%% @private
-spec datastore_info_to_pagination_token(file_meta_forest:list_extended_info()) -> pagination_token().
datastore_info_to_pagination_token(ExtendedInfo) ->
    #pagination_token{
        last_index = build_index(
            ExtendedInfo#list_extended_info.last_name, 
            ExtendedInfo#list_extended_info.last_tree
        ),
        progress_marker = case ExtendedInfo#list_extended_info.is_finished of
            true -> done;
            false -> more
        end,
        datastore_token = ExtendedInfo#list_extended_info.datastore_token
    }.
