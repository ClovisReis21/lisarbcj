const validaDados = (body) => {
    const referenciaRegex = {
        telefone: /\([1-9]{2}\) [9]{0,1}[0-9]{4}\-[0-9]{4}$/,
        cpf: /[0-9]{3}\.[0-9]{3}\.[0-9]{3}\-[0-9]{2}$/,
        email: /^[a-z0-9._]+@[a-z0-9]+\.[a-z]+\.?([a-z]+)?$/i,
        cliente: /^[a-z0-9]{3,}\ [a-z0-9]{3,}$/i,
        estado: /[a-zA-Z]{2}$/i,
        origem_racial: /^indígena|branca|preta|parda|amarela$/i,
        sexo: /^M|F$/i,
        status: /^Silver|Gold|Platinum$/i,
        produto: /^[a-zA-Z á-úÁ-Ú]{5,}$/i, // 'Bomba de pressão',
        preco: /^[0-9]{1,10}$/i,
        nome: /^[a-z0-9]{3,}\ [a-z0-9]{3,}$/i,
    }
    if(Object.keys(body).map((key) => referenciaRegex[key].test(body[key])).includes(false)) return false;
    return true;
}

const validaEntidade = (entidade, body) => {
    let validacao = true;
    const entidadesReferencia = {
        cliente: ['cpf', 'telefone', 'email', 'cliente', 'estado', 'origem_racial', 'sexo', 'status'],
        produto: ['produto', 'preco'],
        vendedor: ['cpf','telefone','email','origem_racial','nome'],
    }
    const keys = Object.keys(body)
    entidadesReferencia[entidade].map(el => {
        if (!keys.includes(el)) validacao = false;
    })
    return validacao;
}

const validar = (entidade, body) => {
    if (!validaEntidade(entidade, body)) return false;
    if (!validaDados(body)) return false;
    return true;
}

module.exports = { validar }