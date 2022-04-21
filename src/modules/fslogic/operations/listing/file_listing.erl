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
%%% When starting a new listing, the `optimize_continuous_listing` parameter must be provided. 
%%% If the optimization is used, there is no guarantee that changes on file tree performed 
%%% after the start of first listing will be included. Therefore it shouldn't be used when 
%%% listing result is expected to be up to date with pagination_token of file tree at the moment of listing 
%%% or when listed batch processing time is substantial (cache timeout is adjusted by cluster_worker's 
%%% `fold_cache_timeout` env variable).
%%% @end
%%%-------------------------------------------------------------------
-module(file_listing).
-author("Michal Stanisz").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/datastore/datastore_runner.hrl").

-export([list/2]).
-export([build_index/1, build_index/2]).
-export([encode_pagination_token/1, decode_pagination_token/1]). 
-export([is_finished/1, get_last_listed_filename/1]).
-export([prepare_result/3]).

-record(list_index, {
    file_name :: file_meta:name(),
    tree_id :: file_meta_forest:tree_id() | undefined
}).

-record(pagination_token, {
    last_index :: undefined | index(),
    datastore_token :: undefined | datastore_list_token(),
    is_finished = false :: boolean()
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
    limit => limit()
} | #{
    optimize_continuous_listing := boolean(),
    index => index(),
    offset => offset(),
    %% @TODO VFS-9283 implement inclusive option in datastore links listing
    inclusive => boolean(),
    whitelist => undefined | whitelist(),
    limit => limit()
}.
%% @formatter:on

-type datastore_options() :: file_meta_forest:list_opts().
-type encoded_pagination_token() :: binary().
-type entry() :: file_meta_forest:link().

-export_type([offset/0, limit/0, index/0, pagination_token/0, options/0]).


-define(DEFAULT_LS_BATCH_SIZE, op_worker:get_env(default_ls_batch_size, 5000)).

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
build_index(undefined, undefined) ->
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


-spec is_finished(pagination_token()) -> boolean().
is_finished(#pagination_token{is_finished = IsFinished}) -> IsFinished.


-spec get_last_listed_filename(pagination_token()) -> file_meta:name().
get_last_listed_filename(#pagination_token{last_index = #list_index{file_name = Name}}) -> Name.


-spec prepare_result([T], file_meta:name() | undefined, limit()) -> 
    {[T], pagination_token()}.
prepare_result(Result, _LastName, Limit) when length(Result) < Limit ->
    {Result, #pagination_token{is_finished = true}};
prepare_result(Result, LastName, _Limit) ->
    {Result, #pagination_token{is_finished = false, last_index = build_index(LastName)}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec list_internal(atom(), file_meta:uuid(), options(), [any()]) -> 
    {ok, [entry()], pagination_token()} | {error, term()}.
list_internal(_FunctionName, _FileUuid, #{limit := 0, pagination_token := PaginationToken}, _ExtraArgs) ->
    {ok, [], PaginationToken};
list_internal(_FunctionName, _FileUuid, #{pagination_token := #pagination_token{is_finished = true}} = PaginationToken, _ExtraArgs) ->
    {ok, [], PaginationToken};
list_internal(_FunctionName, _FileUuid, #{limit := 0} = ListOpts, _ExtraArgs) ->
    {ok, [], #pagination_token{
        last_index = maps:get(index, ListOpts, undefined),
        datastore_token = maps:get(token, convert_to_datastore_options(ListOpts), undefined),
        is_finished = false
    }};
list_internal(FunctionName, FileUuid, ListOpts, ExtraArgs) ->
    ?run(begin
        DatastoreOpts = ensure_starting_point(convert_to_datastore_options(ListOpts)),
        case erlang:apply(file_meta_forest, FunctionName, [FileUuid, DatastoreOpts | ExtraArgs]) of
            {ok, Result, ExtendedInfo} ->
                {ok, Result, build_result_pagination_token(ExtendedInfo)};
            {error, _} = Error ->
                Error
        end
    end).


%% @private
-spec check_exclusive_options(options()) -> ok | no_return().
check_exclusive_options(#{pagination_token := _} = Options) ->
    maps_utils:is_empty(maps:with([optimize_continuous_listing, index, offset, inclusive], Options)) orelse
        %% TODO VFS-7208 introduce conflicting options error after introducing API errors to fslogic
    throw(?EINVAL),
    ok;
check_exclusive_options(_Options) ->
    ok.


%% @private
-spec convert_to_datastore_options(options()) -> datastore_options().
convert_to_datastore_options(#{pagination_token := PaginationToken} = Opts) ->
    #pagination_token{
        last_index = Index,
        datastore_token = DatastoreToken
    } = PaginationToken,
    BaseOpts = index_to_datastore_opts(Index),
    maps_utils:remove_undefined(BaseOpts#{
        token => DatastoreToken,
        size => sanitize_limit(Opts)
    });
convert_to_datastore_options(Opts) ->
    BaseOpts = index_to_datastore_opts(maps:get(index, Opts, undefined)),
    DatastoreToken = case maps:find(optimize_continuous_listing, Opts) of
        {ok, true} -> 
            #link_token{};
        {ok, false} -> 
            undefined;
        error ->
            %% TODO VFS-7208 uncomment after introducing API errors to fslogic
            %% throw(?ERROR_MISSING_REQUIRED_VALUE(optimize_continuous_listing)),
            throw(?EINVAL)
    end,
    Whitelist = maps:get(whitelist, Opts, undefined),
    maps_utils:remove_undefined(BaseOpts#{
        size => sanitize_limit(Opts),
        offset => sanitize_offset(maps:get(offset, Opts, undefined), BaseOpts, Whitelist),
        inclusive => sanitize_inclusive(Opts),
        token => DatastoreToken
    }).


%% @private
-spec ensure_starting_point(datastore_options()) -> datastore_options().
ensure_starting_point(InternalOpts) ->
    % at least one of: offset, token, prev_link_name must be defined so that we know
    % where to start listing
    case maps_utils:is_empty(maps:with([offset, token, prev_link_name], InternalOpts)) of
        false -> InternalOpts;
        true -> InternalOpts#{offset => 0}
    end.


%% @private
-spec sanitize_limit(options()) -> limit().
sanitize_limit(Opts) ->
    case maps:get(limit, Opts, ?DEFAULT_LS_BATCH_SIZE) of
        Size when is_integer(Size) andalso Size >= 0 ->
            Size;
        %% TODO VFS-7208 uncomment after introducing API errors to fslogic
        %% Size when is_integer(Size) ->
        %%     throw(?ERROR_BAD_VALUE_TOO_LOW(size, 0));
        _ ->
            %% TODO VFS-7208 uncomment after introducing API errors to fslogic
            %% throw(?ERROR_BAD_VALUE_INTEGER(size))
            throw(?EINVAL)
    end.


%% @private
-spec sanitize_inclusive(options()) -> boolean().
sanitize_inclusive(Opts) ->
    case maps:get(inclusive, Opts, false) of
        Inclusive when is_boolean(Inclusive) ->
            Inclusive;
        _ ->
            %% TODO VFS-7208 uncomment after introducing API errors to fslogic
            %% throw(?ERROR_BAD_VALUE_BOOLEAN(inclusive))
            throw(?EINVAL)
    end.


%% @private
-spec sanitize_offset(offset() | undefined | any(), datastore_options(), whitelist()) -> 
    offset() | undefined.
sanitize_offset(undefined, _DatastoreOpts, _Whitelist) ->
    undefined;
sanitize_offset(Offset, DatastoreOpts, Whitelist) when is_integer(Offset) ->
    case maps:get(prev_link_name, DatastoreOpts, undefined) of
        undefined ->
            % if prev_link_name is undefined, offset cannot be negative
            %% throw(?ERROR_BAD_VALUE_TOO_LOW(size, 0));
            Offset < 0 andalso throw(?EINVAL);
        _ ->
            % if whitelist is provided, offset cannot be negative
            Whitelist =/= undefined andalso Offset < 0 andalso throw(?EINVAL)
    end,
    Offset;
sanitize_offset(_, _DatastoreOpts, _NegOffsetPolicy) ->
    %% TODO VFS-7208 uncomment after introducing API errors to fslogic
    %% throw(?ERROR_BAD_VALUE_INTEGER(size))
    throw(?EINVAL).


%% @private
-spec index_to_datastore_opts(index() | undefined | binary() | any()) -> map() | no_return().
index_to_datastore_opts(undefined) ->
    #{};
index_to_datastore_opts(#list_index{file_name = Name, tree_id = TreeId}) ->
    #{
        prev_link_name => Name,
        prev_tree_id => TreeId
    };
index_to_datastore_opts(_) ->
    %% TODO VFS-7208 uncomment after introducing API errors to fslogic
    %% throw(?ERROR_BAD_VALUE_INDEX())
    throw(?EINVAL).


%% @private
-spec build_result_pagination_token(file_meta_forest:list_extended_info()) -> pagination_token().
build_result_pagination_token(ExtendedInfo) ->
    Index = case maps:get(last_name, ExtendedInfo, undefined) of
        undefined -> undefined;
        LastName -> #list_index{
            file_name = LastName,
            tree_id = maps:get(last_tree, ExtendedInfo, undefined)
        }
    end,
    #pagination_token{
        last_index = Index,
        is_finished = maps:get(is_last, ExtendedInfo),
        datastore_token = maps:get(token, ExtendedInfo, undefined)
    }.
