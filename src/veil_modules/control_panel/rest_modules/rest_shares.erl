%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements rest_module_behaviour and handles all
%% REST requests directed at /rest/shares/* (creating and deleting shares,
%% listing shares and retrieving share details).
%% @end
%% ===================================================================

-module(rest_shares).
-behaviour(rest_module_behaviour).

-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/control_panel/rest_utils.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao_share.hrl").
-include("veil_modules/dao/dao_users.hrl").
-include("logging.hrl").

-export([methods_and_versions_info/1, content_types_provided/3]).
-export([exists/3, get/2, get/3, delete/3, validate/4, post/4, put/4]).


%% ====================================================================
%% Behaviour callback functions
%% ====================================================================


%% methods_and_versions_info/2
%% ====================================================================
%% @doc Should return list of tuples, where each tuple consists of version of API version and
%% list of methods available in the API version.
%% e.g.: `[{<<"1.0">>, [<<"GET">>, <<"POST">>]}]'
%% @end
-spec methods_and_versions_info(req()) -> {[{binary(), [binary()]}], req()}.
%% ====================================================================
methods_and_versions_info(Req, Id) ->
    {[{<<"1.0">>, [<<"GET">>, <<"POST">>, <<"DELETE">>]]}], Req}.


%% content_types_provided/3
%% ====================================================================
%% @doc Will be called when processing a GET request.
%% Should return list of provided content-types, taking into account (if needed):
%%   - version
%%   - requested ID
%% Should return empty list if given request cannot be processed.
%% @end
-spec content_types_provided(req(), binary(), binary()) -> {[binary()], req()}.
%% ====================================================================
content_types_provided(Req, _Version, _Id) ->
    {[<<"application/json">>], Req}.


%% exists/3
%% ====================================================================
%% @doc Should return whether resource specified by given ID exists.
%% Will be called for GET, PUT and DELETE when ID is contained in the URL. 
%% @end
-spec exists(req(), binary(), binary()) -> {boolean(), req()}.
%% ====================================================================
exists(Req, _Version, Id) ->
    ShareID = binary_to_list(Id),
    case logical_files_manager:get_share({uuid, ShareID}) of
        {ok, _} ->
            {true, Req};
        _ ->
            {false, Req}
    end.


%% get/3
%% ====================================================================
%% @doc Will be called for GET requests. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec get(req(), binary(), binary()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
get(Req, <<"1.0">>, Id) ->
    Response = case Id of
                   undefined ->
                       {ok, #veil_document{uuid = UserID}} = user_logic:get_user({dn, erlang:get(user_id)}),
                       ShareList = case logical_files_manager:get_share({user, UserID}) of
                                       {ok, List} when is_list(List) -> List;
                                       {ok, Doc} -> [Doc];
                                       _ -> []
                                   end,
                       UUIDList = lists:map(
                           fun(#veil_document{uuid = ShareId}) ->
                               list_to_binary(ShareId)
                           end, ShareList),
                       Body = rest_utils:encode_to_json({array, UUIDList}),
                       {body, Body};
                   _ ->
                       try
                           ShareID = binary_to_list(Id),
                           {ok, #veil_document{record = #share_desc{file = FileID}}} = logical_files_manager:get_share({uuid, ShareID}),
                           {ok, Filepath} = logical_files_manager:get_file_user_dependent_name_by_uuid(FileID),
                           BinFilepath = list_to_binary(Filepath),
                           DownloadPath = share_id_to_download_path(Req, Id),
                           Response = rest_utils:encode_to_json({struct, [{file_path, BinFilepath}, {download_path, DownloadPath}]}),
                           {body, Body}
                       catch
                           _:_ ->
                               {error, <<"unknown error">>}
                       end
               end,
    {Response, Req}.


%% delete/3
%% ====================================================================
%% @doc Will be called for DELETE request on given ID. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec delete(req(), binary(), binary()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
delete(Req, <<"1.0">>, Id) ->
    ShareID = binary_to_list(Id),
    case logical_files_manager:remove_share({uuid, ShareID}) of
        ok ->
            {ok, Req};
        _ ->
            {error, <<"unknown">>}
    end.


%% post/4
%% ====================================================================
%% @doc Will be called for POST request. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec post(req(), binary(), binary(), term()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
post(Req, <<"1.0">>, undefined, Data) ->
    try
        Filepath = binary_to_list(Data),
        {ok, ShareID} = case logical_files_manager:get_share({file, Filepath}) of
                            {ok, #veil_document{uuid = UUID}} ->
                                {ok, list_to_binary(UUID)};
                            _ ->
                                {ok, NewUUID} = logical_files_manager:create_standard_share(Filepath),
                                {ok, list_to_binary(NewUUID)}
                        end,
        {{ok, share_id_to_download_path(Req, ShareID)}, Req}
    catch _:_ ->
        {{error, <<"unknown error">>}, Req}
    end.


%% put/4
%% ====================================================================
%% @doc Will be called for PUT request on given ID. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec put(req(), binary(), binary(), term()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
put(Req, <<"1.0">>, _Id, _Data) ->
    {error, Req}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% share_id_to_download_path/2
%% ====================================================================
%% @doc Generates download path based on requested hostname and share id
%% @end
-spec share_id_to_download_path(req(), binary()) -> {boolean(), req()}.
%% ====================================================================
share_id_to_download_path(Req, ShareID) ->
    {Headers, _} = cowboy_req:headers(Req),
    FullHostname = proplists:get_value(<<"host">>, Headers),
    RequestedHostname = case string:tokens(binary_to_list(FullHostname), ":") of
                            [Hostname, _Port] -> list_to_binary(Hostname);
                            [Hostname] -> list_to_binary(Hostname)
                        end,
    SharedFilesPath = list_to_binary(?shared_files_download_path),
    <<"https://", RequestedHostname/binary, SharedFilesPath/binary, ShareID/binary>>.
