%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides high level DB API for handling user documents.
%% @end
%% ===================================================================
-module(dao_users).

-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/dao_helper.hrl").
-include_lib("veil_modules/dao/dao_types.hrl").


%% ===================================================================
%% API functions
%% ===================================================================
-export([save_user/1, remove_user/1, get_user/1]). 


%% save_user/1
%% ====================================================================
%% @doc Saves user to DB. Argument should be either #user{} record
%% (if you want to save it as new document) <br/>
%% or #veil_document{} that wraps #user{} if you want to update descriptor in DB. <br/>
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec save_user(User :: user_info() | user_doc()) -> {ok, user()} | {error, any()} | no_return().
%% ====================================================================
save_user(#user{} = User) ->
    save_user(#veil_document{record = User});
save_user(#veil_document{record = #user{}} = FdDoc) ->
    dao:set_db(?USERS_DB_NAME),
    dao:save_record(FdDoc).


%% remove_user/1
%% ====================================================================
%% @doc Removes user from DB by login, e-mail, uuid or dn.
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec remove_user(Key:: {login, Login :: string()} | 
                        {email, Email :: string()} | 
                        {uuid, UUID :: uuid()} | 
                        {dn, DN :: string()}) -> 
    {error, any()} | no_return().
%% ====================================================================
remove_user(Key) ->
    {ok, FDoc} = get_user(Key),
    dao:remove_record(FDoc#veil_document.uuid).


%% get_user/1
%% ====================================================================
%% @doc Gets user from DB by login, e-mail, uuid or dn.
%% Non-error return value is always {ok, #veil_document{record = #user}.
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec get_user(Key::    {login, Login :: string()} | 
                        {email, Email :: string()} | 
                        {uuid, UUID :: uuid()} | 
                        {dn, DN :: string()}) -> 
    {ok, user_doc()} | {error, any()} | no_return().
%% ====================================================================
get_user({uuid, UUID}) ->
    dao:set_db(?USERS_DB_NAME),
    dao:get_record(UUID);

get_user({Key, Value}) ->
    dao:set_db(?USERS_DB_NAME),

    {View, QueryArgs} = case Key of
        login -> 
            {?USER_BY_LOGIN_VIEW, #view_query_args{keys = 
                [dao_helper:name(Value)], include_docs = true}};
        email -> 
            {?USER_BY_EMAIL_VIEW, #view_query_args{keys = 
                [dao_helper:name(Value)], include_docs = true}};
        dn -> 
            {?USER_BY_DN_VIEW, #view_query_args{keys = 
                [dao_helper:name(Value)], include_docs = true}}
    end,

    case dao:list_records(View, QueryArgs) of
        {ok, #view_result{rows = [#view_row{doc = FDoc}]}} ->
            {ok, FDoc};
        {ok, #view_result{rows = []}} ->
            lager:error("User by ~p: ~p not found", [Key, Value]),
            throw(user_not_found);
        {ok, #view_result{rows = [#view_row{doc = FDoc} | Tail]}} ->
            lager:warning("User ~p is duplicated. Returning first copy. Others: ~p", [FDoc#veil_document.record#user.login, Tail]),
            {ok, FDoc};
        Other ->
            lager:error("Invalid view response: ~p", [Other]),
            throw(invalid_data)
    end.
