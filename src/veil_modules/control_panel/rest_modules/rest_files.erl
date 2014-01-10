%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements rest_module_behaviour and handles all
%% REST requests directed at /rest/file/*. Essentially, it serves 
%% user content (files) via HTTP.
%% @end
%% ===================================================================

-module(rest_files).
-behaviour(rest_module_behaviour).

-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").

-export([allowed_methods/3, content_types_provided/2, content_types_provided/3]).
-export([exists/3, get/2, get/3, delete/3, validate/4, post/4, put/4]).
% optional callback
-export([handle_multipart_data/4]).


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
             undefined -> [<<"GET">>];
             _ -> [<<"GET">>, <<"PUT">>, <<"POST">>, <<"DELETE">>]
           end,
  {Answer, Req}.

%% content_types_provided/2
%% ====================================================================
%% @doc Should return list of provided content-types
%% without specified ID (e.g. ".../rest/resource/"). 
%% Should take into account different types of methods (PUT, GET etc.), if needed.
%% Should return empty list if method is not supported.
%%
%% If there is no id, only dirs can be listed -> application/json.
%% @end
-spec content_types_provided(req(), binary()) -> {[binary()], req()}.
%% ====================================================================
content_types_provided(Req, _Version) ->
    % No id -> lists user's root
    % remember for further use
    erlang:put(file_type, dir),
    {[<<"application/json">>], Req}.


%% content_types_provided/3
%% ====================================================================
%% @doc Should return list of provided content-types
%% with specified ID (e.g. ".../rest/resource/some_id"). Should take into
%% account different types of methods (PUT, GET etc.), if needed.
%% Should return empty list if method is not supported.
%%
%% Id is a dir -> application/json
%% Id is a regular file -> `<mimetype>'
%% Id does not exist -> []
%% @end
-spec content_types_provided(req(), binary(), binary()) -> {[binary()], req()}.
%% ====================================================================
content_types_provided(Req, _Version, Id) ->
    Filepath = binary_to_list(Id),
    Answer = case logical_files_manager:getfileattr(Filepath) of
        {ok, Attr} -> 
            % Remember file attrs not to ask DB again later
            erlang:put(file_attr, Attr),
            case Attr#fileattributes.type of
                "REG" -> 
                    % As the existence is checked here, the result might be remembered and reused
                    erlang:put(file_type, reg),
                    _Mimetypes = mimetypes:path_to_mimes(Filepath);
                "DIR" -> 
                    % As the existence is checked here, the result might be remembered and reused
                    erlang:put(file_type, dir),
                    [<<"application/json">>]
            end;
        _ -> 
            % getfileattr returned error, resource does not exist
            erlang:put(file_type, none),
            % return some content-type so exists function is called and reply returns 404 not found
            [<<"application/json">>]
    end,
    {Answer, Req}.


%% exists/3
%% ====================================================================
%% @doc Should return whether resource specified by given ID exists.
%% Will be called for GET, PUT and DELETE when ID is contained in the URL. 
%% @end
-spec exists(req(), binary(), binary()) -> {boolean(), req()}.
%% ====================================================================
exists(Req, _Version, _Id) -> 
    % Use the knowledge gathered in content_types_provided
    % so as not to repeat code
    {erlang:get(file_type) /= none, Req}.


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
    case user_logic:get_user({dn, erlang:get(user_id)}) of
        {ok, _} ->
            Response = list_dir_to_json("/"),
            {Response, Req};
        _ -> 
            {halt, Req}
    end.


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
    Filepath = binary_to_list(Id),
    % File type was checked in content_types_provided
    case erlang:get(file_type) of
        dir ->
            case user_logic:get_user({dn, erlang:get(user_id)}) of
                {ok, _} ->
                    Response = list_dir_to_json(Filepath),
                    {Response, Req};

                _ -> 
                    {rest_utils:encode_to_json(<<"error: user non-existent in database">>), Req}
            end;
        reg ->
            % File attrs were cached in content_types_provided
            Fileattr = erlang:get(file_attr),
            Size = Fileattr#fileattributes.size,
            BufferSize = file_transfer_handler:get_download_buffer_size(),
            StreamFun = fun(Socket, Transport) ->
                stream_file(Socket, Transport, Filepath, Size, BufferSize)
            end,
            Filename = filename:basename(Filepath),
            HeaderValue = "attachment;" ++
                % Replace spaces with underscores
                " filename=" ++ re:replace(Filename, " ", "_", [global, {return, list}]) ++
                % Offer safely-encoded UTF-8 filename for browsers supporting it
                "; filename*=UTF-8''" ++ http_uri:encode(Filename),

            NewReq = cowboy_req:set_resp_header("content-disposition", HeaderValue, Req),
            {{stream, Size, StreamFun}, NewReq}
    end.


%% validate/4
%% ====================================================================
%% @doc Should return true/false depending on whether the request is valid
%% in terms of the handling module. Will be called before POST or PUT,
%% should discard unprocessable requests.
%% @end
-spec validate(req(), binary(), binary(), term()) -> {boolean(), req()}.
%% ====================================================================
validate(Req, _Version, _Id, _Data) ->
    {true, Req}.


%% delete/3
%% ====================================================================
%% @doc Will be called for DELETE request on given ID. Should try to remove 
%% specified resource and return true/false indicating the result.
%% Should always return false if the method is not supported.
%% @end
-spec delete(req(), binary(), binary()) -> {boolean(), req()}.
%% ====================================================================
delete(Req, _Version, Id) ->
    Filepath = binary_to_list(Id),
    case logical_files_manager:exists(Filepath) of
        true ->
            case logical_files_manager:getfileattr(Filepath) of
                {ok, Attr} ->
                    case Attr#fileattributes.type of
                        "REG" -> case logical_files_manager:delete(Filepath) of
                                     ok -> {true, Req};
                                     {_, _Error} -> {false, Req}
                                 end;
                        _ -> {false, Req}
                    end;
                _ -> {false, Req}
            end;
        _ -> {false, Req}
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
post(Req, _Version, _Id, _Data) -> 
    {false, Req}.


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


%% handle_multipart_data/4
%% ====================================================================
%% @doc Optional callback to handle multipart requests. Data should be streamed
%% in handling module with use of cowboy_multipart module. Method can be `<<"POST">> or <<"PUT">>'.
%% Should handle the request and return true/false indicating the result.
%% Should always return false if the method is not supported.
%% @end
-spec handle_multipart_data(req(), binary(), binary(), term()) -> {boolean(), req()}.
%% ====================================================================
handle_multipart_data(Req, _Version, Method, Id) ->
    case Method of
        <<"POST">> -> file_transfer_handler:handle_rest_upload(Req, Id, false);
        <<"PUT">> -> file_transfer_handler:handle_rest_upload(Req, Id, true);
        _ -> {false, Req}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================


% TODO przeniesc do jakiejs wspolnej lokacji jak n2o zostanie zintegrowane
%% stream_file/5
%% ====================================================================
%% @doc Cowboy style steaming function used to serve file via HTTP.
%% @end
-spec stream_file(term(), atom(), string(), integer(), integer()) -> ok.
%% ====================================================================
stream_file(Socket, Transport, Filepath, Size, BufferSize) ->
    stream_file(Socket, Transport, Filepath, Size, 0, BufferSize).

stream_file(Socket, Transport, Filepath, Size, Sent, BufferSize) ->
    {ok, BytesRead} = logical_files_manager:read(Filepath, Sent, BufferSize),
    ok = Transport:send(Socket, BytesRead),
    NewSent = Sent + size(BytesRead),
    if
        NewSent =:= Size -> ok;
        true -> stream_file(Socket, Transport, Filepath, Size, NewSent, BufferSize)
    end.


%% list_dir_to_json/1
%% ====================================================================
%% @doc Lists the directory and returns response in JSON.
%% @end
-spec list_dir_to_json(string()) -> ok.
%% ====================================================================
list_dir_to_json(Path) ->
    Result = case list_dir(Path) of 
        {error, not_a_dir} -> 
            <<"error: not a dir">>;
        DirList -> 
            DirListBin = lists:map(
                fun(Dir) -> 
                    list_to_binary(Dir)
                end, DirList),
            {array, DirListBin}
    end,
    rest_utils:encode_to_json(Result). 


%% list_dir/1
%% ====================================================================
%% @doc List the given directory, calling itself recursively if there is more to fetch.
%% @end
-spec list_dir(string()) -> [string()].
%% ====================================================================
list_dir(Path) ->
    list_dir(Path, 0, 10, []).

list_dir(Path, Offset, Count, Result) ->
    case logical_files_manager:ls(Path, Count, Offset) of
        {ok, FileList} ->
            case length(FileList) of
                Count -> list_dir(Path, Offset + Count, Count * 10, Result ++ FileList);
                _ -> Result ++ FileList
            end;
        _ ->
            {error, not_a_dir}
    end.