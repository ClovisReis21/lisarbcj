require('dotenv').config();
const { cadastro } = require('./cadastro');
const { validar } = require('./validar');
const { autoGenVendas } = require('./gerador')
const PORTA = process.env.LOJA_SIMULADA_PORTA;

const routes = (app) => {

    app.post('/cadastro/:entidade', async (request, response) => {
        const entidade = request.params?.entidade
        if (!['cliente','produto','vendedor','venda'].includes(entidade))
            return await response.send(400);
        if ((entidade == 'venda' && Object.keys(request.body).length > 0)
            || ( Object.keys(request.body).length > 0 && !validar(entidade, request.body)))
            return await response.send(400);
        const resposta = await cadastro(entidade, request.body)
        if (!resposta) return await response.sendStatus(406).json();
        response.send(201,{idCadastro: resposta}).json();
    });

    app.put('/update/:vendasPorMinuto', async (request, response) => {
        const vendasPorMinuto = request.params?.vendasPorMinuto
        if (isNaN(vendasPorMinuto))
            return await response.send(400);
        autoGenVendas(vendasPorMinuto)
        response.send(200)
    })

    app.listen(PORTA, () => console.log(`Loja Simulada rodando na porta ${PORTA}`,
        process.env.DB_HOST
        ,process.env.DB_USER
        ,process.env.DB_PASS
        ,process.env.DB_NAME
        ,process.env.KAFKA_HOST
        ,process.env.KAFKA_PORT
    ));
}

module.exports = {
    routes
}