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

-export([allowed_methods/3, content_types_provided/2, content_types_provided/3]).
-export([exists/3, get/2, get/3, delete/3, validate/4, post/4, put/4]).


%% ====================================================================
%% Behaviour callback functions
%% ====================================================================

%% allowed_methods/3
%% ====================================================================
%% @doc Should return list of methods that are allowed and directed at specific Id.
%% e.g.: if Id =:= undefined -> `[<<"GET">>, <<"POST">>]'
%%       if Id  /= undefined -> `[<<"GET">>, <<"PUT">>, <<"DELETE">>]'
%% @end
-spec allowed_methods(req(), binary(), binary()) -> {[binary()], req()}.
%% ====================================================================
allowed_methods(Req, _Version, Id) ->
    Answer = case Id of
        undefined -> [<<"GET">>, <<"POST">>];
        _ -> [<<"GET">>, <<"DELETE">>]
    end,
    {Answer, Req}.


%% content_types_provided/2
%% ====================================================================
%% @doc Should return list of provided content-types
%% without specified ID (e.g. ".../rest/resource/"). 
%% Should take into account different types of methods (PUT, GET etc.), if needed.
%% Should return empty list if method is not supported.
%% @end
-spec content_types_provided(req(), binary()) -> {[binary()], req()}.
%% ====================================================================
content_types_provided(Req, _Version) ->
    {Method, _ } = cowboy_req:method(Req),
    Answer = case Method of
        <<"GET">> -> [<<"application/json">>];
        <<"POST">> -> [<<"application/json">>];
        _ -> []
    end,
    {Answer, Req}.


%% content_types_provided/3
%% ====================================================================
%% @doc Should return list of provided content-types
%% with specified ID (e.g. ".../rest/resource/some_id"). Should take into
%% account different types of methods (PUT, GET etc.), if needed.
%% Should return empty list if method is not supported.
%% @end
-spec content_types_provided(req(), binary(), binary()) -> {[binary()], req()}.
%% ====================================================================
content_types_provided(Req, _Version, _Id) ->
    {Method, _ } = cowboy_req:method(Req),
    Answer = case Method of
        <<"GET">> -> [<<"application/json">>];
        <<"DELETE">> -> [<<"application/json">>];
        _ -> []
    end,
    {Answer, Req}.


%% exists/3
%% ====================================================================
%% @doc Should return whether resource specified by given ID exists.
%% Will be called for GET, PUT and DELETE when ID is contained in the URL. 
%% @end
-spec exists(req(), binary(), binary()) -> {boolean(), req()}.
%% ====================================================================
exists(Req, _Version, Id) -> 
    try
        UUID = binary_to_list(Id),
        {ok, _} = logical_files_manager:get_share({uuid, UUID}),
        {true, Req}
    catch _:_ ->
        {false, Req}
    end.


%% get/2
%% ====================================================================
%% @doc Will be called for GET request without specified ID 
%% (e.g. ".../rest/resource/"). Should return one of the following:
%% 1. ResponseBody, of the same type as content_types_provided/1 returned 
%%    for this request
%% 2. Cowboy type stream function, serving content of the same type as 
%%    content_types_provided/1 returned for this request
%% 3. 'halt' atom if method is not supported
%% @end
-spec get(req(), binary()) -> {term() | {stream, integer(), function()} | halt, req(), req()}.
%% ====================================================================
get(Req, _Version) -> 
    {ok, #veil_document { uuid=UserID } } = user_logic:get_user({dn, erlang:get(user_id)}),
    ShareList = case logical_files_manager:get_share({user, UserID}) of
        {ok, List} when is_list(List)-> List;
        {ok, Doc} -> [Doc];
        _ -> []
    end,
    UUIDList = lists:map(
        fun(#veil_document { uuid=ShareId }) ->
            list_to_binary(ShareId)
        end, ShareList), 
    Response = rest_utils:encode_to_json({array, UUIDList}), 
    {Response, Req}.


%% get/3
%% ====================================================================
%% @doc Will be called for GET request with specified ID
%% (e.g. ".../rest/resource/some_id"). Should return one of the following:
%% 1. ResponseBody, of the same type as content_types_provided/2 returned 
%%    for this request
%% 2. Cowboy type stream function, serving content of the same type as 
%%    content_types_provided/2 returned for this request
%% 3. 'halt' atom if method is not supported
%% @end
-spec get(req(), binary(), binary()) -> {term() | {stream, integer(), function()} | halt, req(), req()}.
%% ====================================================================
get(Req, _Version, Id) -> 
    try
        ShareID = binary_to_list(Id),
        {ok, #veil_document { record=#share_desc { file=FileID } } } = logical_files_manager:get_share({uuid, ShareID}),
        {ok, Filepath} = logical_files_manager:get_file_user_dependent_name_by_uuid(FileID),
        BinFilepath = list_to_binary(Filepath),
        DownloadPath = share_id_to_download_path(Req, Id),
        Response = rest_utils:encode_to_json({struct, [{file_path, BinFilepath}, {download_path, DownloadPath}]}),
        {Response, Req}
    catch _:_ ->
        {halt, Req}
    end.


%% validate/4
%% ====================================================================
%% @doc Should return true/false depending on whether the request is valid
%% in terms of the handling module. Will be called before POST or PUT,
%% should discard unprocessable requests.
%%
%% No need to check if file exists as the same will be done in post method
%% @end
-spec validate(req(), binary(), binary(), term()) -> {boolean(), req()}.
%% ====================================================================
validate(Req, _Version, _Id, _Data) ->
    {Method, _ } = cowboy_req:method(Req),
    Answer = case Method of
        <<"POST">> -> true;
        _ -> false
    end,
    {Answer, Req}.


%% delete/3
%% ====================================================================
%% @doc Will be called for DELETE request on given ID. Should try to remove 
%% specified resource and return true/false indicating the result.
%% Should always return false if the method is not supported.
%% @end
-spec delete(req(), binary(), binary()) -> {boolean(), req()}.
%% ====================================================================
delete(Req, _Version, Id) -> 
    try
        ShareID = binary_to_list(Id),
        ok = logical_files_manager:remove_share({uuid, ShareID}),
        % TODO w kolejnej wersji RESTa nalezy uwzglednic callback delete_completed zeby cos zwrocic
        % {rest_utils:encode_to_json({struct, [{ok, true}]}), Req}
        {true, Req}
    catch _:_ ->
        {false, Req}
    end.


%% post/4
%% ====================================================================
%% @doc Will be called for POST request, after the request has been validated. 
%% Should handle the request and return true/false indicating the result.
%% Should always return false if the method is not supported.
%% Returning {true, URL} will cause the reply to contain 201 redirect to given URL.
%% @end
-spec post(req(), binary(), binary(), term()) -> {boolean() | {true, binary()}, req()}.
%% ====================================================================
post(Req, _Version, _Id, Data) -> 
    try
        Filepath = binary_to_list(Data),
        {ok, ShareID} = case logical_files_manager:get_share({file, Filepath}) of
            {ok, #veil_document { uuid=UUID } } -> 
                {ok, list_to_binary(UUID)};
            _ ->
                {ok, NewUUID} = logical_files_manager:create_standard_share(Filepath),
                {ok, list_to_binary(NewUUID)}
        end,
        {{true, share_id_to_download_path(Req, ShareID)}, Req}
    catch _:_ ->
        {false, Req}
    end.


%% put/4
%% ====================================================================
%% @doc Will be called for PUT request on given ID, after the request has been validated. 
%% Should handle the request and return true/false indicating the result.
%% Should always return false if the method is not supported.
%% @end
-spec put(req(), binary(), binary(), term()) -> {boolean(), req()}.
%% ====================================================================
put(Req, _Version, _Id, _Data) -> 
    {false, Req}.


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
