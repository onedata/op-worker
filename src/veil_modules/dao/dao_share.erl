%% ===================================================================
%% @author Michał Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides high level DB API for handling file sharing.
%% @end
%% ===================================================================
-module(dao_share).

-include("veil_modules/dao/dao_share.hrl").
-include_lib("veil_modules/dao/dao_types.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("dao/include/dao_helper.hrl").


%% ===================================================================
%% API functions
%% ===================================================================
-export([save_file_share/1, remove_file_share/1, exist_file_share/1, get_file_share/1]).


%% save_file_share/1
%% ====================================================================
%% @doc Saves info about file sharing to DB. Argument should be either #share_desc{} record
%% (if you want to save it as new document) <br/>
%% or #veil_document{} that wraps #share_desc{} if you want to update descriptor in DB. <br/>
%% See {@link dao_records:save_record/1} and {@link dao_records:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec save_file_share(Share :: file_share_info() | file_share_doc()) -> {ok, file_share()} | {error, any()} | no_return().
%% ====================================================================
save_file_share(#share_desc{} = Share) ->
  save_file_share(#veil_document{record = Share});
save_file_share(#veil_document{record = #share_desc{}} = FdDoc) ->
  dao_external:set_db(?FILES_DB_NAME),
  dao_records:save_record(FdDoc).

%% remove_file_share/1
%% ====================================================================
%% @doc Removes info about file sharing from DB by share_id, file name or user uuid.
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec remove_file_share(Key:: {file, File :: uuid()} |
                        {user, User :: uuid()} |
                        {uuid, UUID :: uuid()}) ->
    {error, any()} | no_return().
%% ====================================================================
remove_file_share({uuid, UUID}) ->
  dao_external:set_db(?FILES_DB_NAME),
  dao_records:remove_record(UUID);

remove_file_share(Key) ->
  {ok, Docs} = get_file_share(Key),
  case Docs of
    D when is_record(D, veil_document) ->
      remove_file_share({uuid, D#veil_document.uuid});
    _ ->
      Rem = fun(Doc, TmpAns) ->
        RemAns = remove_file_share({uuid, Doc#veil_document.uuid}),
        case RemAns of
          ok -> TmpAns;
          _ -> error
        end
      end,
      ListAns = lists:foldl(Rem, ok, Docs),
      case ListAns of
        ok -> ok;
        _ -> throw(remove_file_share_error)
      end
  end.

%% exist_file_share/1
%% ====================================================================
%% @doc Checks whether file share exists in db. Arguments should by share_id, file name or user uuid.                                  l
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec exist_file_share(Key:: {file, File :: uuid()} | {user, User :: uuid()} |
{uuid, UUID :: uuid()}) -> {ok, true | false} | {error, any()}.
%% ====================================================================
exist_file_share({uuid, UUID}) ->
    dao_external:set_db(?FILES_DB_NAME),
    dao_records:exist_record(UUID);
exist_file_share({Key, Value}) ->
    dao_external:set_db(?FILES_DB_NAME),
    {View, QueryArgs} = case Key of
                            user ->
                                {?SHARE_BY_USER_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}};
                            file ->
                                {?SHARE_BY_FILE_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}}
                        end,
    case dao_records:list_records(View, QueryArgs) of
        {ok, #view_result{rows = [#view_row{doc = _FDoc} | _Tail]}} ->
            {ok, true};
        {ok, #view_result{rows = []}} ->
            {ok, false};
        Other -> Other
    end.

%% get_file_share/1
%% ====================================================================
%% @doc Gets info about file sharing from db by share_id, file name or user uuid.                                  l
%% Non-error return value is always {ok, #veil_document{record = #share_desc}.
%% See {@link dao_records:save_record/1} and {@link dao_records:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec get_file_share(Key:: {file, File :: uuid()} |
{user, User :: uuid()} |
{uuid, UUID :: uuid()}) ->
  {ok, file_share_doc()} | {ok, [file_share_doc()]} | {error, any()} | no_return().
%% ====================================================================
get_file_share({uuid, UUID}) ->
  dao_external:set_db(?FILES_DB_NAME),
  dao_records:get_record(UUID);

get_file_share({Key, Value}) ->
  dao_external:set_db(?FILES_DB_NAME),

  {View, QueryArgs} = case Key of
                        user ->
                          {?SHARE_BY_USER_VIEW, #view_query_args{keys =
                          [dao_helper:name(Value)], include_docs = true}};
                        file ->
                          {?SHARE_BY_FILE_VIEW, #view_query_args{keys =
                          [dao_helper:name(Value)], include_docs = true}}
                      end,

  case dao_records:list_records(View, QueryArgs) of
    {ok, #view_result{rows = [#view_row{doc = FDoc}]}} ->
      {ok, FDoc};
    {ok, #view_result{rows = []}} ->
%%       lager:error("Share by ~p: ~p not found", [Key, Value]),
      throw(share_not_found);
    {ok, #view_result{rows = Rows}} ->
      TranslateResponse = fun(#view_row{doc = FDoc}) ->
        FDoc
      end,
      {ok, lists:map(TranslateResponse, Rows)};
    Other ->
      lager:error("Invalid view response: ~p", [Other]),
      throw(invalid_data)
  end.

