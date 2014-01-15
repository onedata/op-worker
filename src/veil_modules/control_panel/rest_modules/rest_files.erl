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

-export([methods_and_versions_info/1, content_types_provided/3]).
-export([exists/3, get/2, get/3, delete/3, validate/4, post/4, put/4]).
% optional callback
-export([handle_multipart_data/4]).


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
    {[{<<"1.0">>, [<<"GET">>, <<"PUT">>, <<"POST">>, <<"DELETE">>]}], Req}.


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
content_types_provided(Req, _Version, Id) ->
    Answer = case Id of
                 undefined ->
                     erlang:put(file_type, dir),
                     [<<"application/json">>];
                 _ ->
                     Filepath = binary_to_list(Id),
                     case logical_files_manager:getfileattr(Filepath) of
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


%% get/3
%% ====================================================================
%% @doc Will be called for GET requests. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec get(req(), binary(), binary()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
get(Req, <<"1.0">>, Id) ->
    FilePath = case Id of
                   undefined -> "/";
                   P -> binary_to_list(P)
               end,

    case user_logic:get_user({dn, erlang:get(user_id)}) of
        {ok, _} ->
            case erlang:get(file_type) of
                dir ->
                    {{body, list_dir_to_json(FilePath)}, Req};
                reg ->
                    % File attrs were cached in content_types_provided
                    Fileattr = erlang:get(file_attr),
                    Size = Fileattr#fileattributes.size,
                    BufferSize = file_transfer_handler:get_download_buffer_size(),
                    StreamFun = fun(Socket, Transport) ->
                        stream_file(Socket, Transport, FilePath, Size, BufferSize)
                    end,
                    Filename = filename:basename(FilePath),
                    HeaderValue = "attachment;" ++
                        % Replace spaces with underscores
                        " filename=" ++ re:replace(Filename, " ", "_", [global, {return, list}]) ++
                        % Offer safely-encoded UTF-8 filename for browsers supporting it
                        "; filename*=UTF-8''" ++ http_uri:encode(Filename),

                    NewReq = cowboy_req:set_resp_header("content-disposition", HeaderValue, Req),
                    {{stream, Size, StreamFun}, NewReq}
            end;
        _ ->
            {{error, <<"error: user non-existent in database">>}, Req}
    end.


%% delete/3
%% ====================================================================
%% @doc Will be called for DELETE request on given ID. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec delete(req(), binary(), binary()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
delete(Req, <<"1.0">>, Id) ->
    Filepath = binary_to_list(Id),
    Response = case erlang:get(file_type) of
                   dir ->
                       {error, <<"its a dir">>};
                   reg ->
                       case logical_files_manager:delete(Filepath) of
                           ok -> {ok, Req};
                           _ -> {error, <<"unknown">>}
                       end
               end,
    {Response, Req}.


%% post/4
%% ====================================================================
%% @doc Will be called for POST request. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec post(req(), binary(), binary(), term()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
post(Req, <<"1.0">>, _Id, _Data) ->
    {error, Req}.


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


%% handle_multipart_data/4
%% ====================================================================
%% @doc Optional callback to handle multipart requests. Data should be streamed
%% in handling module with use of cowboy_multipart module. Method can be `<<"POST">> or <<"PUT">>'.
%% Must return one of answers described in rest_module_behaviour.
%% @end
-spec handle_multipart_data(req(), binary(), binary(), term()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
handle_multipart_data(Req, _Version, Method, Id) ->
    case Method of
        <<"POST">> -> file_transfer_handler:handle_rest_upload(Req, Id, false);
        <<"PUT">> -> file_transfer_handler:handle_rest_upload(Req, Id, true)
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