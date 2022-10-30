var MongoClient = require('mongodb').MongoClient;
const pdfparse = require('pdf-parse');
const moment = require('moment');
var log4js = require('log4js');
var request = require("request");

const api_token = '';
const api_chat_url = 'https://api.ultramsg.com/instance9469/messages/chat';
const api_doc_url = 'https://api.ultramsg.com/instance9469/messages/document';
const api_link_url = 'https://api.ultramsg.com/instance9469/messages/link';

log4js.configure({
    appenders: {
        out:{ type: 'console' },
        app:{ type: 'file', filename: 'logs/sendwhatsapp.log' }
    },
    categories: {
        default: { appenders: [ 'out', 'app' ], level: 'debug' }
    }
});

var mongo_url = 'mongodb://datarg:password@127.0.0.1:27017/datarg';

var logger = log4js.getLogger(); 
let dbclient;

function sendMessage(options) {
    return new Promise(function(resolve, reject) {
        request(options, function (error, response, body) {
            if (error) {
                logger.warn(error);
                reject(error);
            } else {
                resolve(body);
            }
        });
    });
}

function updateRequestStatus(id, status) {
    return new Promise(function(resolve, reject) {
        var newStatus = { $set: { status: status } };
        MongoClient.connect(mongo_url, (errdb, db) => {
            if (!errdb) {
                dbclient = db;
                var res = db.db('datarg').collection('requests').updateOne({ _id: id }, newStatus, function(err, result){
                    if (err) {
                        logger.warn(err);
                        reject(err);
                    } else {
                        resolve(result);
                    }
                });
            } else {
                reject(errdb);
            }
        });
    });
}

function updateRequestPhone(id, phone) {
    return new Promise(function(resolve, reject) {
        var newStatus = { $set: { phone: phone } };
        MongoClient.connect(mongo_url, (errdb, db) => {
            if (!errdb) {
                dbclient = db;
                var res = db.db('datarg').collection('requests').updateOne({ _id: id }, newStatus, function(err, result){
                    if (err) {
                        logger.warn(err);
                        reject(err);
                    } else {
                        resolve(result);
                    }
                });
            } else {
                reject(errdb);
            }
        });
    });
}

function searchPhoneInSales(dni) {
    return new Promise((resolve, reject) => {
        MongoClient.connect(mongo_url, (errdb, db) => {
            if(!errdb) {
                dbclient = db;
                var res = db.db("datarg").collection("sales").find({ dni: dni }).sort({ $natural: -1 }).toArray(function(err, result){
                    if (err) {
                        logger.warn(err);
                        reject(err);
                    } else {
                        resolve(result);
                    }
                });
            } else {
                reject(errdb);
            }
        });
    });
}

function getDNIfromTad(tad) {
    return new Promise((resolve, reject) => {
        pdfparse(tad).then(function(data) {
            var extracted = data.text.split("\n");
            extracted.forEach(txt => {
                if ((txt.length == 8 || txt.length == 7) && /^\d+$/.test(txt)) {
                    dni = txt;
                    logger.debug('DNI Encontrado: '+ dni);
                    resolve(dni);
                }
            });
        });
    });
}

function getDNIfromCert(cert) {
    return new Promise((resolve, reject) => {
        pdfparse(cert).then(function(data) {
            var extracted = data.text.split("\n");
            extracted.forEach(txt => {
                if (txt.includes('D.N.I.:')){
                    txt = txt.replace(/\D/g, "");
                    if ((txt.length == 8 || txt.length == 7 || txt.length == 6) && /^\d+$/.test(txt)) {
                        dni = txt;
                        logger.debug('DNI Encontrado: '+ dni);
                        resolve(dni);
                    }
                }
            });
        });
    });
}

function findReqCodesToSend() {
    return new Promise((resolve, reject) => {
        MongoClient.connect(mongo_url, (errdb, db) => {
            if (!errdb) {
                dbclient = db;
                var res = db.db('datarg').collection('requests').find({"$and": [{"request_code": {"$exists": true}},{"file_cert": {"$exists": false}},{"security_code": {"$exists": false}},{"$or": [{"status": "0"},{"status": "2"}]}]} , {"_id": 1,"request_code": 1, "phone": 1, "file_tad": 1}).toArray(function(err, result){
                    if (err) {
                        logger.warn(err);
                        reject(err);
                    } else {
                        resolve(result);
                    }
                });
            } else {
                reject(errdb);
            }
        });
    });
}

function findSecCodesToSend() {
    return new Promise((resolve, reject) => {
        MongoClient.connect(mongo_url, (errdb, db) => {
            if (!errdb) {
                dbclient = db;
                var res = db.db('datarg').collection('requests').find({"$and": [{"request_code": {"$exists": true}},{"security_code": {"$exists": true}},{"file_cert": {"$exists": false}},{"$or": [{"status": "0"},{"status": "1"},{"status": "2"},{"status": "4"}]}]} , {"_id": 1,"request_code": 1,"security_code": 1,"phone": 1,"file_tad": 1}).toArray(function(err, result){
                    if (err) {
                        logger.warn(err);
                        reject(err);
                    } else {
                        resolve(result);
                    }
                });
            } else {
                reject(errdb);
            }
        });
    });
}

function findCertsToSend() {
    return new Promise((resolve, reject) => {
        MongoClient.connect(mongo_url, (errdb, db) => {
            if (!errdb) {
                dbclient = db;
                var res = db.db('datarg').collection('requests').find({"$and":[{"file_cert":{"$exists":true}},{"security_code":{"$exists":true}},{"request_code":{"$exists":true}},{"$or":[{"status":{"$ne":"5"}},{"status":"0"}]}]},{"_id":1,"request_code":1,"security_code":1,"phone":1,"file_cert":1}).toArray(function(err, result){
                    if (err) {
                        logger.warn(err);
                        reject(err);
                    } else {
                        resolve(result);
                    }
                });
            } else {
                reject(errdb);
            }
        });
    });
}

async function sendMessages() {
    var ReqCodes = await findReqCodesToSend();

    if (dbclient) {
        if (dbclient.topology.isConnected()) {
            dbclient.close();
        }
    }

    if (ReqCodes.length > 0) {
        logger.debug('Códigos TAD a enviar: ' + ReqCodes.length);

        for await (var req of ReqCodes){
            var id = req._id;
            var request_code = req.request_code;

            if (req.phone) {
                logger.debug('Cel. encontrado, método 1');
                var phone = req.phone;

                var options = {
                    method: 'POST',
                    url: api_chat_url,
                    headers: {'content-type': 'application/x-www-form-urlencoded'},
                    form: {
                        token: api_token,
                        to: phone,
                        body: 'Hola. Le informamos que su solicitud para la tramitación del Certificado de Antecedentes Penales ante el Registro Nacional de Reinsidencia Nro. ' + request_code + ', modalidad TAD (trámite a distancia) ha sido registrada exitosamente. \n\n*IMPORTANTE*: el plazo de entrega del Certificado de Antecedentes Penales es de 24 a 48hs en días hábiles de 8 a 18hs.',
                        priority: '10',
                        referenceId: ''
                    }
                };

                logger.info('ENVIANDO CÓDIGO TAD: ' + request_code +  ' A: ' + phone);
                
                await (async  () => {
                    var response = await sendMessage(options);

                    response = JSON.parse(response);
                
                    if (response['sent'] == 'true') {
                        logger.info('CÓDIGO TAD ENVIADO CORRECTAMENTE A: ' + phone);
                        var update = await updateRequestStatus(id, "1");
                        
                        if (dbclient) {
                            if (dbclient.topology.isConnected()) {
                                dbclient.close();
                            }
                        }

                        logger.debug('UPDATE STATUS OK');
                    } else {
                        logger.warn('ERROR ENVIANDO CÓDIGO TAD A: ' + phone);
                        var update = await updateRequestStatus(id, "2");
                        
                        if (dbclient) {
                            if (dbclient.topology.isConnected()) {
                                dbclient.close();
                            }
                        }

                        logger.debug('UPDATE ERROR STATUS OK');
                    }
                })();
            } else if (req.file_tad && !req.phone) {
                logger.debug('Cel. No encontrado. Intentando método 2');
                
                await (async  () => {
                    var buffer_tad = Buffer.from(req.file_tad, 'base64');
                    var dni = await getDNIfromTad(buffer_tad);
                    var phones = await searchPhoneInSales(dni);

                    if (dbclient) {
                        if (dbclient.topology.isConnected()) {
                            dbclient.close();
                        }
                    }
    
                    if (phones.length > 0) {
                        var phone = phones[0].phone;
    
                        var options = {
                            method: 'POST',
                            url: api_chat_url,
                            headers: {'content-type': 'application/x-www-form-urlencoded'},
                            form: {
                                token: api_token,
                                to: phone,
                                body: 'Hola. Le informamos que su solicitud para la tramitación del Certificado de Antecedentes Penales ante el Registro Nacional de Reinsidencia Nro. ' + request_code + ', modalidad TAD (trámite a distancia) ha sido registrada exitosamente. \n\n*IMPORTANTE*: el plazo de entrega del Certificado de Antecedentes Penales es de 24 a 48hs en días hábiles de 8 a 18hs.',
                                priority: '10',
                                referenceId: ''
                            }
                        };
    
                        logger.info('ENVIANDO CÓDIGO TAD: ' + request_code +  ' A: ' + phone);
                        var message_response = await sendMessage(options);
                        message_response = JSON.parse(message_response);
                    
                        if (message_response['sent'] == 'true') {
                            logger.info('CÓDIGO TAD ENVIADO CORRECTAMENTE A: '  + phone);
                            var update = await updateRequestStatus(id, "1");
                            
                            if (dbclient) {
                                if (dbclient.topology.isConnected()) {
                                    dbclient.close();
                                }
                            }

                            logger.debug('UPDATE STATUS OK');

                            update = await updateRequestPhone(id, phone);

                            if (dbclient) {
                                if (dbclient.topology.isConnected()) {
                                    dbclient.close();
                                }
                            }

                            logger.debug('UPDATE PHONE OK');
                        } else {
                            logger.warn('ERROR ENVIANDO CÓDIGO TAD A: ' + phone);
                            var update = await updateRequestStatus(id, "2");
                            
                            if (dbclient) {
                                if (dbclient.topology.isConnected()) {
                                    dbclient.close();
                                }
                            }

                            logger.debug('UPDATE ERROR STATUS OK');
                            update = await updateRequestPhone(id, phone);
                            
                            if (dbclient) {
                                if (dbclient.topology.isConnected()) {
                                    dbclient.close();
                                }
                            }
                        }
                    } else {
                        logger.warn('CEL. No encontrado\n');
                    }
                })();
            }
        };
    }

    var SecCodes = await findSecCodesToSend();
    
    if (dbclient) {
        if (dbclient.topology.isConnected()) {
            dbclient.close();
        }
    }

    if (SecCodes.length > 0) {
        logger.debug('Códigos de seguridad a enviar: ' + SecCodes.length);
        for await (var req of SecCodes){
            var id = req._id;
            var request_code = req.request_code;
            var security_code = req.security_code;

            if (req.phone) {
                logger.debug('Cel. encontrado, método 1');
                var phone = req.phone;

                var options = {
                    method: 'POST',
                    url: api_chat_url,
                    headers: {'content-type': 'application/x-www-form-urlencoded'},
                    form: {
                        token: api_token,
                        to: phone,
                        body: 'Estimado Usuario, le enviamos los datos necesarios para consultar el estado del Certificado Digital de Antecedentes Penales emitido por el Registro Nacional de Reinsidencia\n\n*Código de la solicitud:* ' + request_code + ' \n\n*Código de Seguridad:* ' + security_code + '\n\nDeberá realizar la consulta ingresando a: ',
                        priority: '10',
                        referenceId: ''
                    }
                };

                logger.info('ENVIANDO CÓDIGO DE SOLICITUD: ' + request_code +  ' Y DE SEGURIDAD: ' + security_code + ' A: ' + phone);
                
                await (async  () => {
                    var response = await sendMessage(options);

                    response = JSON.parse(response);
                
                    if (response['sent'] == 'true') {
                        logger.info('CÓDIGOS ENVIADOS CORRECTAMENTE A: ' + phone);
                        
                        options = {
                            method: 'POST',
                            url: api_link_url,
                            headers: {'content-type': 'application/x-www-form-urlencoded'},
                            form: {token: api_token, to: phone, link: 'http://www.dnrec.jus.gov.ar/ConsultaCAP', referenceId: ''}
                        };
                        response = await sendMessage(options);

                        var update = await updateRequestStatus(id, "3");

                        if (dbclient) {
                            if (dbclient.topology.isConnected()) {
                                dbclient.close();
                            }
                        }

                        logger.debug('UPDATE STATUS OK');
                    } else {
                        logger.warn('ERROR ENVIANDO CÓDIGOS A: ' + phone);
                        var update = await updateRequestStatus(id, "4");
                        
                        if (dbclient) {
                            if (dbclient.topology.isConnected()) {
                                dbclient.close();
                            }
                        }

                        logger.debug('UPDATE ERROR STATUS OK');
                    }
                })();
            } else if (req.file_tad && !req.phone) {
                logger.debug('(Req&Sec Codes) Cel. No encontrado. Intentando método 2');
                
                await (async  () => {
                    var buffer_tad = Buffer.from(req.file_tad, 'base64');
                    var dni = await getDNIfromTad(buffer_tad);
                    var phones = await searchPhoneInSales(dni);

                    if (dbclient) {
                        if (dbclient.topology.isConnected()) {
                            dbclient.close();
                        }
                    }
    
                    if (phones.length > 0) {
                        var phone = phones[0].phone;

                        var options = {
                            method: 'POST',
                            url: api_chat_url,
                            headers: {'content-type': 'application/x-www-form-urlencoded'},
                            form: {
                                token: api_token,
                                to: phone,
                                body: 'Estimado Usuario, le enviamos los datos necesarios para consultar el estado del Certificado Digital de Antecedentes Penales emitido por el Registro Nacional de Reinsidencia\n\n*Código de la solicitud:* ' + request_code + ' \n\n*Código de Seguridad:* ' + security_code + '\n\nDeberá realizar la consulta ingresando a: ',
                                priority: '10',
                                referenceId: ''
                            }
                        };
        
                        logger.info('ENVIANDO CÓDIGO DE SOLICITUD: ' + request_code +  ' Y DE SEGURIDAD: ' + security_code + ' A: ' + phone);
                        var response = await sendMessage(options);

                        response = JSON.parse(response);
                    
                        if (response['sent'] == 'true') {
                            logger.info('CÓDIGOS ENVIADOS CORRECTAMENTE A: ' + phone);
                            
                            options = {
                                method: 'POST',
                                url: api_link_url,
                                headers: {'content-type': 'application/x-www-form-urlencoded'},
                                form: {token: api_token, to: phone, link: 'http://www.dnrec.jus.gov.ar/ConsultaCAP', referenceId: ''}
                            };
                            response = await sendMessage(options);
    
                            var update = await updateRequestStatus(id, "3");
                            
                            if (dbclient) {
                                if (dbclient.topology.isConnected()) {
                                    dbclient.close();
                                }
                            }

                            logger.debug('UPDATE STATUS OK');

                            update = await updateRequestPhone(id, phone);
                            
                            if (dbclient) {
                                if (dbclient.topology.isConnected()) {
                                    dbclient.close();
                                }
                            }

                            logger.debug('UPDATE PHONE OK');
                        } else {
                            logger.warn('ERROR ENVIANDO CÓDIGOS A: ' + phone);
                            var update = await updateRequestStatus(id, "4");
                            
                            if (dbclient) {
                                if (dbclient.topology.isConnected()) {
                                    dbclient.close();
                                }
                            }

                            logger.debug('UPDATE ERROR STATUS OK');
                        }
                    } else {
                        logger.warn('CEL. No encontrado\n');
                    }
                })();
            }
        }
    }

    var CertsToSend = await findCertsToSend();

    if (dbclient) {
        if (dbclient.topology.isConnected()) {
            dbclient.close();
        }
    }

    if (CertsToSend.length > 0) {
        logger.debug('Certificados a enviar: ' + CertsToSend.length);
        for await (var req of CertsToSend){
            var id = req._id;
            var file_cert = req.file_cert;
            var tad_code = req.request_code;
            logger.debug('Codigo TAD: ' + tad_code);
            if(req.phone) {
                logger.debug('Cel. encontrado, método 1');
                var phone = req.phone;

                var options = {
                    method: 'POST',
                    url: api_chat_url,
                    headers: {'content-type': 'application/x-www-form-urlencoded'},
                    form: {
                        token: api_token,
                        to: phone,
                        body: 'Estimado Usuario, le informamos que su Solicitud del Certificado de Antecedentes Penales ante el Registro Nacional de Reinsidencia, ha culminado exitosamente.\n\nSu certificado se encuentra adjunto a continuación:',
                        priority: '10',
                        referenceId: ''
                    }
                };

                logger.info('ENVIANDO CERTIFICADO A: ' + phone);

                await (async  () => {
                    var response = await sendMessage(options);

                    response = JSON.parse(response);
                
                    if (response['sent'] == 'true') {
                        options = {
                            method: 'POST',
                            url: api_doc_url,
                            headers: {'content-type': 'application/x-www-form-urlencoded'},
                            form: {
                                token: api_token,
                                to: phone,
                                filename: 'Certificado.pdf',
                                document: file_cert,
                                referenceId: '',
                                nocache: ''
                            }
                        };

                        response = await sendMessage(options);

                        options = {
                            method: 'POST',
                            url: api_chat_url,
                            headers: {'content-type': 'application/x-www-form-urlencoded'},
                            form: {
                                token: api_token,
                                to: phone,
                                body: 'Muchas gracias por elegirnos! Esperamos poder servirte nuevamente.',
                                priority: '10',
                                referenceId: ''
                            }
                        };

                        response = await sendMessage(options);

                        logger.info('CERTIFICADO ENVIADO CORRECTAMENTE A: ' + phone);
                        
                        var update = await updateRequestStatus(id, "5");
                        
                        if (dbclient) {
                            if (dbclient.topology.isConnected()) {
                                dbclient.close();
                            }
                        }

                        logger.debug('UPDATE STATUS OK');
                    } else {
                        logger.warn('ERROR ENVIANDO CERTIFICADO A: ' + phone);
                        var update = await updateRequestStatus(id, "6");
                        
                        if (dbclient) {
                            if (dbclient.topology.isConnected()) {
                                dbclient.close();
                            }
                        }

                        logger.debug('UPDATE ERROR STATUS OK');
                    }
                })();
            } else if (req.file_cert && !req.phone) {
                logger.debug('(Cert) Cel. No encontrado. Intentando método 2');
                await (async  () => {
                    var buffer_cert = Buffer.from(req.file_cert, 'base64');
                    var dni = await getDNIfromCert(buffer_cert);

                    var phones = await searchPhoneInSales(dni);
                    
                    if (dbclient) {
                        if (dbclient.topology.isConnected()) {
                            dbclient.close();
                        }
                    }

                    if (phones.length > 0) {
                        var phone = phones[0].phone;

                        var options = {
                            method: 'POST',
                            url: api_chat_url,
                            headers: {'content-type': 'application/x-www-form-urlencoded'},
                            form: {
                                token: api_token,
                                to: phone,
                                body: 'Estimado Usuario, le informamos que su Solicitud del Certificado de Antecedentes Penales ante el Registro Nacional de Reinsidencia, ha culminado exitosamente.\n\nSu certificado se encuentra adjunto a continuación:',
                                priority: '10',
                                referenceId: ''
                            }
                        };
        
                        logger.info('ENVIANDO CERTIFICADO A: ' + phone);

                        await (async  () => {
                            var response = await sendMessage(options);

                            response = JSON.parse(response);
                
                            if (response['sent'] == 'true') {
                                options = {
                                    method: 'POST',
                                    url: api_doc_url,
                                    headers: {'content-type': 'application/x-www-form-urlencoded'},
                                    form: {
                                        token: api_token,
                                        to: phone,
                                        filename: 'Certificado.pdf',
                                        document: file_cert,
                                        referenceId: '',
                                        nocache: ''
                                    }
                                };

                                response = await sendMessage(options);

                                options = {
                                    method: 'POST',
                                    url: api_chat_url,
                                    headers: {'content-type': 'application/x-www-form-urlencoded'},
                                    form: {
                                        token: api_token,
                                        to: phone,
                                        body: 'Muchas gracias por elegirnos! Esperamos poder servirte nuevamente.',
                                        priority: '10',
                                        referenceId: ''
                                    }
                                };

                                response = await sendMessage(options);

                                logger.info('CERTIFICADO ENVIADO CORRECTAMENTE A: ' + phone);
                        
                                var update = await updateRequestStatus(id, "5");
                                
                                if (dbclient) {
                                    if (dbclient.topology.isConnected()) {
                                        dbclient.close();
                                    }
                                }

                                logger.debug('UPDATE STATUS OK');

                                update = await updateRequestPhone(id, phone);
                                
                                if (dbclient) {
                                    if (dbclient.topology.isConnected()) {
                                        dbclient.close();
                                    }
                                }

                                logger.debug('UPDATE PHONE OK');
                            } else {
                                logger.warn('ERROR ENVIANDO CERTIFICADO A: ' + phone);
                                var update = await updateRequestStatus(id, "6");
                                
                                if (dbclient) {
                                    if (dbclient.topology.isConnected()) {
                                        dbclient.close();
                                    }
                                }

                                logger.debug('UPDATE ERROR STATUS OK');
                            }
                        })();
                    } else {
                        logger.warn('CEL. No encontrado\n');
                    }
                })();
            }
        }
    }
}

process.setMaxListeners(1000);

process.on('uncaughtException', (e) => {
    console.log(e);
    logger.warn('Error fatal.');
});

async function checkdbconnection () {
    var status = false;
    try {
        var client = await MongoClient.connect(mongo_url);
        client.close();
        status = true;
    } catch (e) {
        status = false;
    }
    return status;
}

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

(async ()=> {
    while(true) {
        var dbstatus = await checkdbconnection();
        if (dbstatus) {
          await sendMessages();
          await delay(10000);
        }
    }
})();
