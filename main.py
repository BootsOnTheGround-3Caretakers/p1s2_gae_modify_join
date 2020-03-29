from __future__ import absolute_import
from __future__ import unicode_literals

import os
import sys

import webapp2
from google.appengine.ext import ndb

cwd = os.getcwd()
sys.path.insert(0, 'includes')
from datavalidation import DataValidation
from GCP_return_codes import FunctionReturnCodes as RC
from task_queue_functions import TaskQueueFunctions
from p1_services import Services, TaskArguments
from p1_global_settings import PostDataRules
from p1_datastores import Datastores
from datastore_functions import DatastoreFunctions as DSF


class CommonPostHandler(DataValidation):
    def post(self):
        task_id = "modify-joins:CommonPostHandler:post"
        debug_data = []
        call_result = self.processPushTask()
        debug_data.append(call_result)
        task_results = call_result['task_results']

        params = {}
        for key in self.request.arguments():
            params[key] = self.request.get(key, None)
        task_functions = TaskQueueFunctions()

        if call_result['success'] != RC.success:
            task_functions.logError(
                call_result['success'], task_id, params,
                self.request.get('X-AppEngine-TaskName', None),
                self.request.get('transaction_id', None), call_result['return_msg'], debug_data,
                self.request.get('transaction_user_uid', None)
            )
            task_functions.logTransactionFailed(self.request.get('transaction_id', None), call_result['success'])
            if call_result['success'] == RC.failed_retry:
                self.response.set_status(500)
            if call_result['success'] == RC.input_validation_failed:
                self.response.set_status(400)
            if call_result['success'] == RC.ACL_check_failed:
                self.response.set_status(401)

            return

        # go to the next function
        task_functions = TaskQueueFunctions()
        call_result = task_functions.nextTask(task_id, task_results, params)
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            task_functions.logError(
                call_result['success'], task_id, params,
                self.request.get('X-AppEngine-TaskName', None),
                self.request.get('transaction_id', None), call_result['return_msg'], debug_data,
                self.request.get('transaction_user_uid', None)
            )
        # </end> go to the next function
        self.response.set_status(200)


class AddModifyNeedToNeeder(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:AddModifyNeedToNeeder:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        need_uid = unicode(self.request.get(TaskArguments.s2t4_need_uid, ""))
        needer_uid = unicode(self.request.get(TaskArguments.s2t4_needer_uid, ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t4_user_uid, ""))
        special_requirements = unicode(self.request.get(TaskArguments.s2t4_special_requests, "")) or None

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [need_uid, Datastores.needer_needs_joins._rule_need_uid],
            [needer_uid, Datastores.needer_needs_joins._rule_needer_uid],
            [user_uid, Datastores.needer_needs_joins._rule_user_uid],
            [special_requirements, Datastores.needer_needs_joins._rule_special_requests],
        ])
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "input validation failed"
            return {
                'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results,
            }

        try:
            existings_keys = [
                ndb.Key(Datastores.needer._get_kind(), long(needer_uid)),
                ndb.Key(Datastores.needs._get_kind(), long(need_uid)),
                ndb.Key(Datastores.users._get_kind(), long(user_uid)),
            ]
        except Exception as exc:
            return_msg += str(exc)
            return {
                'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results,
            }

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result['success'] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    'success': RC.datastore_failure, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        key_name = "{}|{}".format(needer_uid, needer_uid)
        joins = Datastores.needer_needs_joins(id=key_name)
        joins.needer_uid = needer_uid
        joins.need_uid = need_uid
        joins.user_uid = user_uid
        joins.special_requests = special_requirements
        call_result = joins.kput()
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "failed to write needer_need_joins to datastore"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'task_results': task_results
            }

        task_results['uid'] = call_result['put_result'].id()

        return {'success': RC.success, 'return_msg': return_msg, 'debug_data': debug_data, 'task_results': task_results}


app = webapp2.WSGIApplication([
    (Services.modify_joins.add_modify_need_to_needer.url, AddModifyNeedToNeeder),
], debug=True)
