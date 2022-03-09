%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 20222 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module operates on two types of tokens used for continuous listing of directory content: 
%%%     * api_list_token - used by higher-level modules and passed to the external clients as an 
%%%                        opaque string so that they can resume listing just after previously 
%%%                        listed batch (results paging). It encodes a datastore_token and additional
%%%                        information about last position in the file tree, in case the datastore
%%%                        token expires. Hence, this token does not expire and always guarantees
%%%                        correct listing resumption.
%%%                    
%%%     * datastore_list_token - token used internally by datastore, has limited TTL. After its expiration, 
%%%                              information about last position in the file tree can be used to determine
%%%                              the starting point for listing. This token offers the best performance 
%%%                              when resuming listing from a certain point.
%%% @TODO VFS-8980 currently it is possible to pass datastore_token from outside
%%% @end
%%%--------------------------------------------------------------------
-module(file_listing).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").


-type api_list_token() :: binary().
-type list_token() :: file_meta:list_token() | api_list_token().
-type token_type() :: api_list_token | datastore_list_token.

%% @TODO VFS-8980 Create opaque structure for list opts
% When api_list_token is provided, other starting point options (offset, last_tree, last_name) are ignored
-type list_opts() :: #{
    token := api_list_token(),
    size := file_meta:list_size()
} | file_meta:list_opts().


-export_type([list_token/0, list_opts/0]).

%% API
-export([
    list_children/4
]).

-define(MAX_MAP_CHILDREN_PROCESSES, application:get_env(
    ?APP_NAME, max_read_dir_plus_procs, 20
)).
-define(API_LIST_TOKEN_PREFIX, "api_list_token").

%%%===================================================================
%%% API
%%%===================================================================

%% @private
-spec list_children(user_ctx:ctx(), file_ctx:ctx(), list_opts(),
    CanonicalChildrenWhiteList :: undefined | [file_meta:name()]
) ->
    {
        Children :: [file_ctx:ctx()],
        ExtendedInfo :: file_meta:list_extended_info(),
        NewFileCtx :: file_ctx:ctx()
    }.
list_children(UserCtx, FileCtx0, ListOpts, CanonicalChildrenWhiteList) ->
    {TokenType, FinalListOpts} = resolve_list_opts(ListOpts),
    {Children, ExtendedInfo, NewFileCtx} = files_tree:get_children(
        FileCtx0, UserCtx, FinalListOpts, CanonicalChildrenWhiteList),
    FinalExtendedInfo = case TokenType of
        api_list_token -> 
            #{
                is_last => maps:get(is_last, ExtendedInfo),
                token => pack_api_list_token(ExtendedInfo)
            };
        _ ->
            ExtendedInfo
    end,
    {Children, FinalExtendedInfo, NewFileCtx}.


%% @private
-spec resolve_list_opts(list_opts()) -> {token_type() | none, file_meta:list_opts()}.
resolve_list_opts(#{token := undefined} = ListOpts) ->
    {none, ListOpts};
resolve_list_opts(#{token := ?INITIAL_API_LS_TOKEN} = ListOpts) ->
    {api_list_token, #{
        token => ?INITIAL_DATASTORE_LS_TOKEN,
        last_tree => <<>>,
        last_name => <<>>,
        size => maps:get(size, ListOpts)
    }};
resolve_list_opts(#{token := Token} = ListOpts) ->
    try
        {api_list_token, maps:merge(unpack_api_list_token(Token), #{
            size => maps:get(size, ListOpts)
        })}
    catch _:_ ->
        {datastore_list_token, ListOpts}
    end;
resolve_list_opts(ListOpts) ->
    {none, ListOpts}.


%% @private
-spec pack_api_list_token(file_meta:list_extended_info()) -> binary().
pack_api_list_token(#{token := DatastoreToken, last_tree := LastTree, last_name := LastName}) ->
    mochiweb_base64url:encode(str_utils:join_binary(
        [<<?API_LIST_TOKEN_PREFIX>>, DatastoreToken, LastTree, LastName], <<"#">>));
pack_api_list_token(_) ->
    undefined.


%% @private
-spec unpack_api_list_token(api_list_token()) -> map().
unpack_api_list_token(Token) ->
    <<?API_LIST_TOKEN_PREFIX, _/binary>> = DecodedToken = mochiweb_base64url:decode(Token),
    [_Header, DecodedDatastoreToken, LastTree | LastNameTokens] =
        binary:split(DecodedToken, <<"#">>, [global]),
    #{
        token => DecodedDatastoreToken,
        last_tree => LastTree,
        last_name => str_utils:join_binary(LastNameTokens, <<"#">>)
    }.