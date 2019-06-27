const https = require("https");
const querystring = require("querystring");
const crypto = require("crypto");

let ACCESS_TOKEN = null;
let SESSION_ID = null;

const serviceCall = async ({ path, postData, jsonEncoded = true, method = "POST" }) => {
  if (ACCESS_TOKEN === null) {
    ACCESS_TOKEN = await login();
  }
  const options = {
    hostname: process.env.SUITECRM_HOST_NAME,
    port: 443,
    path: path,
    method: method,
    headers: {
      "Content-Type": jsonEncoded ? "application/json" : "application/x-www-form-urlencoded",
      Authorization: "Bearer " + ACCESS_TOKEN
    }
  };

  return new Promise((resolve, reject) => {
    const req = https.request(options, function(res) {
      let body = "";
      console.log(`STATUS: ${res.statusCode}`);
      console.log(`HEADERS: ${JSON.stringify(res.headers)}`);
      res.setEncoding("utf8");
      res.on("data", chunk => {
        body += chunk;
      });
      res.on("end", () => {
        try {
          response = JSON.parse(body);
          resolve(response);
        } catch (e) {
          reject(body);
        }
      });
    });

    req.on("error", e => {
      console.error(`Error en service call: ${e.message}`);
      reject(e);
    });

    console.log("PostData: " + postData);
    req.write(postData);
    req.end();
  });
};

const login = () => {
  const postData = querystring.stringify({
    grant_type: "password",
    client_id: process.env.SUITECRM_SERVICE_CLIENT_ID,
    client_secret: process.env.SUITECRM_SERVICE_SECRET,
    username: process.env.SUITECRM_SERVICE_USER,
    password: process.env.SUITECRM_SERVICE_PWD
  });
  const options = {
    hostname: process.env.SUITECRM_HOST_NAME,
    port: 443,
    path: "/Api/access_token",
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Authorization: "Bearer"
    }
  };
  return new Promise((resolve, reject) => {
    const req = https.request(options, function(res) {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", chunk => (body += chunk));
      res.on("end", () => {
        try {
          response = JSON.parse(body);
          resolve(response.access_token);
        } catch (e) {
          reject(body);
        }
      });
    });
    req.on("error", e => reject(e));
    req.write(postData);
    req.end();
  });
};

const legacyServiceCall = async ({ method, argumentList }) => {
  const postData = querystring.stringify({
    method: method,
    rest_data: argumentList,
    input_type: "JSON",
    response_type: "JSON"
  });

  const options = {
    hostname: process.env.SUITECRM_HOST_NAME,
    port: 443,
    path: "/service/v4_1/rest.php",
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Content-Length": Buffer.byteLength(postData)
    }
  };

  return new Promise((resolve, reject) => {
    const req = https.request(options, function(res) {
      let body = "";
      console.log(`STATUS: ${res.statusCode}`);
      console.log(`HEADERS: ${JSON.stringify(res.headers)}`);
      res.setEncoding("utf8");
      res.on("data", chunk => {
        body += chunk;
      });
      res.on("end", () => {
        try {
          body = JSON.parse(body);
          resolve(body);
        } catch (ex) {
          reject(ex);
        }
      });
    });

    req.on("error", e => {
      console.error(`Error en [legacy] service call: ${e.message}`);
      reject(e);
    });

    console.log("PostData: " + postData);
    req.write(postData);
    req.end();
  });
};

const legacyLogin = async () => {
  if (SESSION_ID !== null) {
    return SESSION_ID;
  }
  const postData = querystring.stringify({
    method: "login",
    rest_data: JSON.stringify({
      user_auth: {
        user_name: process.env.SUITECRM_SERVICE_USER,
        password: crypto
          .createHash("md5")
          .update(process.env.SUITECRM_SERVICE_PWD)
          .digest("hex"),
        version: "1"
      },
      application_name: "vinco360",
      name_value_list: []
    }),
    input_type: "JSON",
    response_type: "JSON"
  });

  const options = {
    hostname: process.env.SUITECRM_HOST_NAME,
    port: 443,
    path: "/service/v4_1/rest.php",
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Content-Length": Buffer.byteLength(postData)
    }
  };

  return new Promise((resolve, reject) => {
    const req = https.request(options, function(res) {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", chunk => (body += chunk));
      res.on("end", () => {
        try {
          body = JSON.parse(body);
          SESSION_ID = body.id;
          resolve(SESSION_ID);
        } catch (ex) {
          reject(ex);
        }
      });
    });
    req.on("error", e => {
      reject(e);
    });
    req.write(postData);
    req.end();
  });
};

module.exports = { serviceCall, legacyServiceCall, legacyLogin };
