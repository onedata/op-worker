%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for providing a higher level API for listing files. It translates 
%%% listing parameters used commonly in op-worker to datastore links parameters. Therefore it operates 
%%% on two types of tokens used for continuous listing of directory content: 
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
%%% On starting a new listing the `optimize_continuous_listing` parameter must be provided. 
%%% When the optimisation is used there is no guarantee that changes on files tree performed 
%%% after first batch is already listed will be included. Therefore it shouldn't be used when 
%%% state of files tree is expected to be up to date at the moment of listing or when listed batch 
%%% processing time is substantial (cache timeout is adjusted by cluster_worker's `fold_cache_timeout` env variable).
%%% @end
%%%-------------------------------------------------------------------
-module(file_listing).
-author("Michal Stanisz").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/datastore/datastore_runner.hrl").

-type pagination_token() :: binary().
-type datastore_list_token() :: binary().
-type offset() :: integer().
-type limit() :: non_neg_integer().

%% @formatter:off
-type options() :: #{
    pagination_token := pagination_token() | undefined,
    limit => limit()
} | #{
    index => index(),
    offset => offset(),
    limit => limit(),
    %% @TODO VFS-9283 implement inclusive option in datastore links listing
    inclusive => boolean(),
    optimize_continuous_listing := boolean()
}.
%% @formatter:on

-record(list_index, {
    name :: file_meta:name(),
    tree_id :: file_meta_forest:tree_id() | undefined
}).

-opaque index() :: #list_index{}.

-record(state, {
    index :: undefined | index(),
    datastore_token :: undefined | datastore_list_token(),
    is_finished = false :: boolean()
}).


-type entry() :: file_meta_forest:link().
-opaque state() :: #state{}.

-export_type([offset/0, limit/0, index/0, pagination_token/0, options/0, state/0]).

-export([list/2, list_whitelisted/3]).
-export([build_index/1, build_index/2]).
-export([build_pagination_token/1, is_finished/1, get_last_listed_filename/1]).
-export([default_state/0, default_state/2]).
-export([build_state/2, build_state/3]).


-define(DEFAULT_LS_BATCH_SIZE, op_worker:get_env(ls_batch_size, 5000)).

%%%===================================================================
%%% API
%%%===================================================================

-spec list(file_meta:uuid(), options()) -> {ok, [entry()], state()} | {error, term()}.
list(FileUuid, ListOpts) ->
    list_internal(?FUNCTION_NAME, FileUuid, ListOpts, [], true).


-spec list_whitelisted(file_meta:uuid(), options(), [file_meta:name()]) ->
    {ok, [entry()], state()} | {error, term()}.
list_whitelisted(FileUuid, ListOpts, WhiteList) ->
    list_internal(?FUNCTION_NAME, FileUuid, ListOpts, [WhiteList], false).


-spec build_index(binary()) -> index().
build_index(Name) ->
    build_index(Name, undefined).


-spec build_index(binary(), binary()) -> index().
build_index(Name, TreeId) when is_binary(Name) andalso is_binary(TreeId) ->
    #list_index{name = Name, tree_id = TreeId};
build_index(_, _) ->
    %% TODO VFS-7208 uncomment after introducing API errors to fslogic
    %% throw(?ERROR_BAD_VALUE_BINARY())
    throw(?EINVAL).


-spec build_pagination_token(state()) -> pagination_token() | undefined.
build_pagination_token(#state{is_finished = true}) ->
    undefined;
build_pagination_token(State) ->
    mochiweb_base64url:encode(term_to_binary(State)).


-spec is_finished(state()) -> boolean().
is_finished(#state{is_finished = IsFinished}) -> IsFinished.


-spec get_last_listed_filename(state()) -> file_meta:name().
get_last_listed_filename(#state{index = #list_index{name = Name}}) -> Name.


-spec default_state() -> state().
default_state() ->
    #state{is_finished = true}.


-spec default_state(file_meta:name(), boolean()) -> state().
default_state(LastName, Finished) ->
    #state{is_finished = Finished, index = #list_index{name = LastName}}. 


-spec build_state(pagination_token(), boolean()) -> state().
build_state(_PaginationToken, true) ->
    default_state();
build_state(PaginationToken, _IsFinished) ->
    pagination_token_to_state(PaginationToken).


-spec build_state(boolean(), file_meta:name(), file_meta_forest:tree_id()) -> state().
build_state(OptimizeContinuousListing, LastName, LastTreeId) ->
    DatastoreToken = case OptimizeContinuousListing of
        true -> #link_token{};
        false -> undefined
    end,
    #state{
        index = build_index(LastName, LastTreeId),
        datastore_token = DatastoreToken,
        is_finished = false
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec list_internal(atom(), file_meta:uuid(), options(), [any()], boolean()) -> 
    {ok, [entry()], state()} | {error, term()}.
list_internal(FunctionName, FileUuid, ListOpts, ExtraArgs, AllowNegOffset) ->
    ?run(begin
        case sanitize_opts(ListOpts, AllowNegOffset) of
            stop ->
                {ok, [], default_state()};
            {continue, SanitizedOpts} ->
                case erlang:apply(file_meta_forest, FunctionName, [FileUuid, SanitizedOpts | ExtraArgs]) of
                    {ok, Result, ExtendedInfo} ->
                        {ok, Result, build_result_state(ExtendedInfo)};
                    {error, _} = Error ->
                        Error
                end
        end
    end).


%% @private
-spec pagination_token_to_state(pagination_token()) -> state().
pagination_token_to_state(Token) ->
    try binary_to_term(base64url:decode(Token)) of
        #state{} = State ->
            State;
        _ ->
            throw(?EINVAL)
    catch _:_ ->
        throw(?EINVAL)
    end.


%% @private
-spec sanitize_opts(options(), boolean()) -> stop | {continue, file_meta_forest:list_opts()}.
sanitize_opts(#{pagination_token := undefined}, _AllowNegOffset) -> 
    stop;
sanitize_opts(#{pagination_token := PaginationToken} = Opts, _AllowNegOffset) ->
    #state{
        index = Index,
        datastore_token = DatastoreToken
    } = pagination_token_to_state(PaginationToken),
    BaseOpts = unpack_index(Index),
    {continue, maps_utils:remove_undefined(BaseOpts#{
        token => DatastoreToken,
        size => sanitize_limit(Opts)
    })};
sanitize_opts(Opts, AllowNegOffset) ->
    BaseOpts = unpack_index(maps:get(index, Opts, undefined)),
    DatastoreToken = case maps:get(optimize_continuous_listing, Opts, undefined) of
        true -> #link_token{};
        false -> undefined;
        undefined ->
            %% TODO VFS-7208 uncomment after introducing API errors to fslogic
            %% throw(?ERROR_MISSING_REQUIRED_VALUE(optimize_continuous_listing)),
            throw(?EINVAL)
    end,
    {continue, validate_starting_opts(maps_utils:remove_undefined(BaseOpts#{
        size => sanitize_limit(Opts),
        offset => sanitize_offset(Opts, AllowNegOffset),
        inclusive => sanitize_inclusive(Opts),
        token => DatastoreToken
    }))}.


%% @private
-spec validate_starting_opts(file_meta_forest:list_opts()) -> file_meta_forest:list_opts().
validate_starting_opts(InternalOpts) ->
    % at least one of: offset, token, prev_link_name must be defined so that we know
    % where to start listing
    case map_size(maps:with([offset, token, prev_link_name], InternalOpts)) > 0 of
        true ->
            InternalOpts;
        false ->
            InternalOpts#{offset => 0}
    end.


%% @private
-spec sanitize_limit(options()) -> limit().
sanitize_limit(Opts) ->
    case maps:get(limit, Opts, undefined) of
        undefined ->
            ?DEFAULT_LS_BATCH_SIZE;
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
    case maps:get(inclusive, Opts, undefined) of
        undefined ->
            false;
        Inclusive when is_boolean(Inclusive) ->
            Inclusive;
        _ ->
            %% TODO VFS-7208 uncomment after introducing API errors to fslogic
            %% throw(?ERROR_BAD_VALUE_BOOLEAN(inclusive))
            throw(?EINVAL)
    end.


%% @private
-spec sanitize_offset(options(), AllowNegative :: boolean()) -> offset() | undefined.
sanitize_offset(Opts, AllowNegative) ->
    case maps:get(offset, Opts, undefined) of
        undefined -> undefined;
        Offset when is_integer(Offset) ->
            LastName = maps:get(index, Opts, undefined),
            case {AllowNegative andalso LastName =/= undefined, Offset >= 0} of
                {true, _} ->
                    Offset;
                {false, true} ->
                    Offset;
                {false, false} ->
                    % if LastName is undefined, Offset cannot be negative
                    %% throw(?ERROR_BAD_VALUE_TOO_LOW(size, 0));
                    throw(?EINVAL)
            end;
        _ ->
            %% TODO VFS-7208 uncomment after introducing API errors to fslogic
            %% throw(?ERROR_BAD_VALUE_INTEGER(size))
            throw(?EINVAL)
    end.


%% @private
-spec unpack_index(index() | undefined | binary() | any()) -> map() | no_return().
unpack_index(#list_index{name = Name, tree_id = TreeId}) ->
    #{
        prev_link_name => Name,
        prev_tree_id => TreeId
    };
unpack_index(undefined) ->
    #{};
unpack_index(Name) when is_binary(Name) ->
    #{
        prev_link_name => Name
    };
unpack_index(_) ->
    %% TODO VFS-7208 uncomment after introducing API errors to fslogic
    %% throw(?ERROR_BAD_VALUE_INDEX())
    throw(?EINVAL).


%% @private
-spec build_result_state(file_meta_forest:list_extended_info()) -> state().
build_result_state(ExtendedInfo) ->
    Index = case maps:get(last_name, ExtendedInfo, undefined) of
        undefined -> undefined;
        LastName -> #list_index{
            name = LastName,
            tree_id = maps:get(last_tree, ExtendedInfo, undefined)
        }
    end,
    #state{
        index = Index,
        is_finished = maps:get(is_last, ExtendedInfo),
        datastore_token = maps:get(token, ExtendedInfo, undefined)
    }.
