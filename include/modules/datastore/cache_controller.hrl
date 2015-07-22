%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Functions' templates used by caches controllers.
%%% @end
%%%-------------------------------------------------------------------

-include_lib("ctool/include/logging.hrl").

-ifndef(CACHE_CONTROLLER_HRL).
-define(CACHE_CONTROLLER_HRL, 1).

-define(UPDATE_USAGE_INFO2(__Cache),
  update_usage_info(Key, ModelName) ->
    Uuid = caches_controller:get_cache_uuid(Key, ModelName),
    UpdateFun = fun(Record) ->
      Record#__Cache{timestamp = os:timestamp()}
    end,
    case update(Uuid, UpdateFun) of
      {ok, Ok} ->
        {ok, Ok};
      {error,{not_found,__Cache}} ->
        TS = os:timestamp(),
        V = #__Cache{timestamp = TS, last_action_time = TS},
        Doc = #document{key = Uuid, value = V},
        create(Doc)
    end
).

-define(UPDATE_USAGE_INFO3(__Level),
  update_usage_info(Key, ModelName, Doc) ->
    update_usage_info(Key, ModelName),
    datastore:create(__Level, Doc),
    datastore:foreach_link(disk_only, Key, ModelName,
      fun(LinkName, LinkTarget, _) ->
        datastore:add_links(__Level, Key, ModelName, {LinkName, LinkTarget})
      end,
      [])
).

-define(CHECK_GET2(__Cache),
  check_get(Key, ModelName) ->
    Uuid = caches_controller:get_cache_uuid(Key, ModelName),
    case get(Uuid) of
      {ok, Doc} ->
        Value = Doc#document.value,
        case Value#__Cache.action of
          delete -> {error, {not_found, ModelName}};
          _ -> ok
        end;
      {error, {not_found, _}} ->
        ok
    end
).

-define(CHECK_GET3(__Cache),
  check_get(Key, ModelName, LinkName) ->
    Uuid = caches_controller:get_cache_uuid(Key, ModelName),
    case get(Uuid) of
      {ok, Doc} ->
        Value = Doc#document.value,
        Links = Value#__Cache.deleted_links,
        case lists:member(LinkName, Links) or lists:member(all, Links) of
          true -> {error, link_not_found};
          _ -> ok
        end;
      {error, {not_found, _}} ->
        ok
    end
).

-define(DELETE_DUMP_INFO(__Cache),
  delete_dump_info(Uuid) ->
    Pid = self(),
    Pred = fun() ->
      LastUser = case get(Uuid) of
                   {ok, Doc} ->
                     Value = Doc#document.value,
                     Value#__Cache.last_user;
                   {error, {not_found, _}} ->
                     non
                 end,
      case LastUser of
        Pid ->
          true;
        _ ->
          false
      end
    end,
    delete(Uuid, Pred)
).

-define(END_DICK_OP(__Cache),
  end_disk_op(Key, ModelName, Op) ->
    try
      Uuid = caches_controller:get_cache_uuid(Key, ModelName),
      case Op of
        delete ->
          delete_dump_info(Uuid);
        _ ->
          UpdateFun = fun
            (#__Cache{last_user = LastUser} = Record) ->
              case LastUser of
                Pid ->
                  Record#__Cache{last_user = non, action = non, last_action_time = os:timestamp()};
                _ ->
                  throw(user_changed)
              end
          end,
          update(Uuid, UpdateFun)
      end,
      ok
    catch
      E1:E2 ->
        ?error_stacktrace("Error in ~p end_disk_op. Args: ~p. Error: ~p:~p.", 
          [{__Cache, Key, ModelName, Op}, E1, E2]),
        {error, ending_disk_op_failed}
    end
).

-define(START_DISK_OP(__Cache, __Level),
  start_disk_op(Key, ModelName, Op) ->
    try
      Uuid = caches_controller:get_cache_uuid(Key, ModelName),
      Pid = self(),
  
      UpdateFun = fun(Record) ->
        Record#__Cache{last_user = Pid, timestamp = os:timestamp(), action = Op}
      end,
      case update(Uuid, UpdateFun) of
        {ok, Ok} ->
          {ok, Ok};
        {error,{not_found,__Cache}} ->
          TS = os:timestamp(),
          V = #__Cache{last_user = Pid, timestamp = TS, action = Op, last_action_time = TS},
          Doc = #document{key = Uuid, value = V},
          create(Doc)
      end,
  
      {ok, SleepTime} = application:get_env(?APP_NAME, cache_to_disk_delay_ms),
      timer:sleep(SleepTime),
      Uuid = caches_controller:get_cache_uuid(Key, ModelName),
  
      {LastUser, LAT} = case get(Uuid) of
                          {ok, Doc2} ->
                            Value = Doc2#document.value,
                            {Value#__Cache.last_user, Value#__Cache.last_action_time};
                          {error, {not_found, _}} ->
                            {Pid, 0}
                        end,
      case LastUser of
        Pid ->
          case Op of
            delete ->
              ok;
            _ ->
              {ok, SavedValue} = datastore:get(__Level, ModelName, Key),
              {ok, save, [SavedValue]}
          end;
        _ ->
          {ok, ForceTime} = application:get_env(?APP_NAME, cache_to_disk_force_delay_ms),
          case timer:now_diff(os:timestamp(), LAT) >= 1000*ForceTime of
            true ->
              UpdateFun2 = fun(Record) ->
                Record#__Cache{last_action_time = os:timestamp()}
              end,
              update(Uuid, UpdateFun2),
              {ok, SavedValue} = datastore:get(__Level, ModelName, Key),
              {ok, save, [SavedValue]};
            _ ->
              {error, not_last_user}
          end
      end
    catch
      E1:E2 ->
        ?error_stacktrace("Error in ~p start_disk_op. Args: ~p. Error: ~p:~p.",
          [{__Cache, Key, ModelName, Op}, E1, E2]),
        {error, preparing_disk_op_failed}
    end
).

-define(LOG_LINK_DEL(__Cache),
  log_link_del(Key, ModelName, LinkNames, start) ->
    try
      Uuid = caches_controller:get_cache_uuid(Key, ModelName),
  
      UpdateFun = fun(#__Cache{deleted_links = DL} = Record) ->
        case LinkNames of
          LNs when is_list(LNs) ->
            Record#__Cache{deleted_links = DL ++ LinkNames};
          _ ->
            Record#__Cache{deleted_links = DL ++ [LinkNames]}
        end
      end,
      case update(Uuid, UpdateFun) of
        {ok, Ok} ->
          {ok, Ok};
        {error,{not_found,__Cache}} ->
          TS = os:timestamp(),
          V = case LinkNames of
                LNs when is_list(LNs) ->
                  #__Cache{timestamp = TS, deleted_links = LinkNames};
                _ ->
                  #__Cache{timestamp = TS, deleted_links = [LinkNames]}
              end,
          Doc = #document{key = Uuid, value = V},
          create(Doc)
      end
    catch
      E1:E2 ->
        ?error_stacktrace("Error in ~p log_link_del. Args: ~p. Error: ~p:~p.",
          [{__Cache, Key, ModelName, LinkNames, start}, E1, E2]),
        {error, preparing_disk_op_failed}
    end;
  log_link_del(Key, ModelName, LinkNames, stop) ->
    try
      Uuid = caches_controller:get_cache_uuid(Key, ModelName),
      UpdateFun = fun(#__Cache{deleted_links = DL} = Record) ->
        case LinkNames of
          LNs when is_list(LNs) ->
            Record#__Cache{deleted_links = DL -- LinkNames};
          _ ->
            Record#__Cache{deleted_links = DL -- [LinkNames]}
        end
      end,
      update(Uuid, UpdateFun)
    catch
      E1:E2 ->
        ?error_stacktrace("Error in ~p log_link_del. Args: ~p. Error: ~p:~p.",
          [{__Cache, Key, ModelName, LinkNames, stop}, E1, E2]),
        {error, ending_disk_op_failed}
    end
).

-define(BEFORE,
  before(ModelName, save, disk_only, [Doc]) ->
    start_disk_op(Doc#document.key, ModelName, save);
  before(ModelName, update, disk_only, [Key, _Diff]) ->
    start_disk_op(Key, ModelName, update);
  before(ModelName, create, disk_only, [Doc]) ->
    start_disk_op(Doc#document.key, ModelName, create);
  before(ModelName, delete, disk_only, [Key, _Pred]) ->
    start_disk_op(Key, ModelName, delete);
  before(ModelName, get, disk_only, [Key]) ->
    check_get(Key, ModelName);
  before(ModelName, fetch_link, disk_only, [Key, LinkName]) ->
    check_get(Key, ModelName, LinkName);
  before(ModelName, delete_links, disk_only, [Key, LinkNames]) ->
    log_link_del(Key, ModelName, LinkNames, start);
  before(_ModelName, _Method, _Level, _Context) ->
    ok
).

-define(AFTER(__Level),
  'after'(ModelName, save, disk_only, [Doc], {ok, _}) ->
    end_disk_op(Doc#document.key, ModelName, save);
  'after'(ModelName, update, disk_only, [Key, _Diff], {ok, _}) ->
    end_disk_op(Key, ModelName, update);
  'after'(ModelName, create, disk_only, [Doc], {ok, _}) ->
    end_disk_op(Doc#document.key, ModelName, create);
  'after'(ModelName, get, disk_only, [Key], {ok, Doc}) ->
    update_usage_info(Key, ModelName, Doc);
  'after'(ModelName, get, __Level, [Key], {ok, _}) ->
    update_usage_info(Key, ModelName);
  'after'(ModelName, delete, disk_only, [Key, _Pred], ok) ->
    end_disk_op(Key, ModelName, delete);
  'after'(ModelName, delete, __Level, [Key, _Pred], ok) ->
  Uuid = caches_controller:get_cache_uuid(Key, ModelName),
    delete_dump_info(Uuid);
  'after'(ModelName, exists, __Level, [Key], {ok, true}) ->
    update_usage_info(Key, ModelName);
  'after'(ModelName, delete_links, disk_only, [Key, LinkNames], ok) ->
    log_link_del(Key, ModelName, LinkNames, stop);
  'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok
).

-define(LIST_DOCS_TO_BE_DUMPED(__Cache, __Level, MODEL_NAME),
  list_docs_be_dumped() ->
    Filter = fun
      ('$end_of_table', Acc) ->
        {abort, Acc};
      (#document{key = Uuid, value = V}, Acc) ->
        U = V#__Cache.last_user,
        case U =/= non of
          true ->
            {next, [Uuid | Acc]};
          false ->
            {next, Acc}
        end
    end,
    datastore:list(__Level, MODEL_NAME, Filter, [])
).

-define(LIST_OLDER(__Cache, __Level, MODEL_NAME),
  list(DocAge) ->
    Now = os:timestamp(),
    Filter = fun
      ('$end_of_table', Acc) ->
        {abort, Acc};
      (#document{key = Uuid, value = V}, Acc) ->
        T = V#__Cache.timestamp,
        U = V#__Cache.last_user,
        case (timer:now_diff(Now, T) >= 1000*DocAge) and (U =:= non) of
          true ->
            {next, [Uuid | Acc]};
          false ->
            {next, Acc}
        end
    end,
    datastore:list(__Level, MODEL_NAME, Filter, [])
).

-endif.